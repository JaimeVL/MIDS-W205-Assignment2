import sys
from datetime import date, timedelta, datetime
import time
import string
import tweepy
import json
import signal
import os
import pandas as pd
import numpy as np
import nltk
from nltk.corpus import stopwords
import csv
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from matplotlib import pylab

# Used to process tweets and write to JSON files
class TweetSerializer:
    out = None
    first = True
    count = 0
    json_contents = []
    lowest_id = 0
    lowest_datetime = None

    # Write data into disk
    def write(self):
        if self.count == 0: return None

        fname = self.convert_to_string(self.lowest_datetime) + '-' + str(self.lowest_id) + '-' + str(self.count) + '.json'
        print 'Writing file: %s' % fname
        self.out = open('out/' + fname, "w")
        self.out.write("[\n")

        self.first = True
        for entry in self.json_contents:
            if not self.first: self.out.write(",\n")
            else: self.first = False

            self.out.write(json.dumps(entry, indent=2, separators=(',', ': ')).encode('utf8'))

        self.out.write("\n]\n")
        self.out.close()
        self.out = None

        # Throwing away all previous content
        self.json_contents = []
        self.lowest_id = 0
        self.lowest_datetime = None
        self.first = True

        print ' + Finished writing file: %s' % fname

        return fname

    # Get lowest ID found in tweets currently stored in json_contents
    def get_lowest_id(self):
        print ' = Lowest ID: %s' % self.lowest_id
        print ' = Lowest datetime: %s' % self.lowest_datetime # Convert to valid datetime

        return self.lowest_id

    # Add tweet to json_contents by processing it and getting the fields we care about. Also,
    # adds the following values: HasFinalsHashTag, HasWarriorsHashTag, HasBothHashTag.
    def add_tweets(self,tweet):
        self.count += 1

        json_data = { key: tweet._json[key] for key in ['created_at', 'lang', 'text', 'id'] }
        json_data['screen_name'] = tweet._json['user']['screen_name']

        current_lowest_datetime = self.convert_to_float(json_data['created_at'])
        current_lowest_id = json_data['id']

        # 2) Process tweets and get: lowest Id, lowest datetime, hasFinalsHashTag, hasWarriorsHashTag, fileName
        if self.first:
            self.lowest_datetime = current_lowest_datetime
            self.lowest_id = current_lowest_id
        else:
            if current_lowest_datetime <  self.lowest_datetime:
                self.lowest_datetime = current_lowest_datetime

            if current_lowest_id < self.lowest_id:
                self.lowest_id = current_lowest_id

        # Remove later, can be calculated afterwards...
        text = json_data['text'].lower()
        tag1 = ('#nbafinals2015' in text)
        tag2 = ('#warriors' in text)
        json_data['HasFinalsHashTag'] = tag1
        json_data['HasWarriorsHashTag'] = tag2
        json_data['HasBothHashTag'] = tag1 and tag2

        self.first = False
        self.json_contents += [json_data]

    # Convert datetime as it appears in tweet into a float
    def convert_to_float(self, date):
        return time.mktime(time.strptime(date, "%a %b %d %H:%M:%S +0000 %Y"))

    # Convert float into a YYYY-MM-DD string
    def convert_to_string(self, float):
        return datetime.fromtimestamp(float).strftime('%Y-%m-%d')

# General purpose methods
class Utils:
    tokenizer = nltk.RegexpTokenizer('[\S]+')
    stop_words = stopwords.words('english')

    @staticmethod
    def connect_to_S3():
        conn = S3Connection('---------------------', '------------------------------------')
        return conn.get_bucket('jvl-mids-w205-assignment2')

    @staticmethod
    def connect_to_Twitter():
        consumer_key = "------------------------";
        consumer_secret = "-----------------------------------------------------";
        access_token = "-----------------------------------------------------";
        access_token_secret = "-----------------------------------------------------";

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return tweepy.API(auth_handler=auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    @staticmethod
    def query_Twitter(api, ts, date, max_id = 0):
        nextDate = date + timedelta(days = 1)
        query = '#NBAFinals2015 OR #Warriors since:' + date.strftime('%Y-%m-%d') + " until:" + nextDate.strftime('%Y-%m-%d')

        print 'Starting query (max_id = %s)' % max_id
        count = 0
        if max_id == 0:
            for tweet in tweepy.Cursor(api.search, q = query).items(1000):
                count += 1
                ts.add_tweets(tweet)
        else:
            for tweet in tweepy.Cursor(api.search, q = query, max_id = str(max_id)).items(1000):
                count += 1
                ts.add_tweets(tweet)

        print ' - Query done. # of results: %s' % count
        return count

    @staticmethod
    def get_free_disk_space(pathname):
        # Get the free space of the filesystem containing pathname
        stat = os.statvfs(pathname)
        # use f_bfree for superuser, or f_bavail if filesystem has reserved space for superuser
        return (stat.f_bfree*stat.f_bsize) / (1024.0*1024*1024)

    @staticmethod
    def interrupt(signum, frame):
        print 'Interrupted, closing ...'
        if ts.count > 0:
          print 'Since there are contents, they will be written to disk before exiting'
          ts.write()
        exit(1)

    @staticmethod
    def tokenize(df):
        list_of_tokens = [Utils.tokenizer.tokenize(txt) for txt in df]

        all_tokens = []
        for tokens in list_of_tokens:
            # Convert all tokens to lower case
            lower_tokens = [token.lower() for token in tokens]
            # Keep all tokens that are not stopwords, are not URLs, and are not part of the hashtags we searched for
            all_tokens += [token for token in lower_tokens if token not in Utils.stop_words and not token.startswith('http:') and token not in ['#nbafinals2015', '#warriors']]

        return all_tokens

    @staticmethod
    def write_counts_to_csv(fd, filename):
        with open(filename, "wb") as fp:
            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            for item in fd.items():
                writer.writerow([unicode(item[0]).encode("utf-8"), item[1]])

# Get tweets containing #NBAFinals2015 or #Warriors, and stores them in S3
def get_tweets():
    bucket = Utils.connect_to_S3()
    api = Utils.connect_to_Twitter()
    signal.signal(signal.SIGINT, Utils.interrupt) # Used to interrupt execution

    ts = TweetSerializer()

    # Set start, end, and current dates used to track progress of work.
    start_date = date(2015,06,17)
    end_date = date(2015,06,11)
    current_date = start_date

    # Process data one day at a time
    result_count = 0
    previous_result_count = 0
    queries = 0
    max_id = 0

    found_results = True
    print "Current date: %s" % current_date
    while current_date >= end_date:
        # Get tweets based ib max_id value (could be 0, in which case none is used)
        result_count += Utils.query_Twitter(api, ts, current_date, max_id)

        max_id = ts.get_lowest_id() - 1
        queries += 1

        # Sets flag if not results are found
        if previous_result_count == result_count:
            found_results = False
        # Sets flag if results are found and updates result count
        else:
            previous_result_count = result_count
            found_results = True

        # If count is 0, it means we never got any results for this date and we should stop execution.
        if result_count == 0:
            print 'Unexpected results, stopping execution'
            return
        # Write file if there are enough results, or if there were no results in the last query.
        elif (queries % 5 == 0) or not found_results:
            fname = ts.write()

            # Save contents to S3
            key = bucket.new_key('%s/%s' % (current_date, fname))
            key.set_contents_from_filename('out/' + fname)
            print 'Stored contents in S3'

            # Delete JSON file
            os.remove('out/' + fname)
            print 'Deleted local JSON file'

            result_count = 0
            previous_result_count = 0

            # Check that there is still enough disk space
            if Utils.get_free_disk_space('/') < 10.0:
                print 'Free disk space is less than 10 GB'
                return

        print 'Completed query #%s. Sleeping for 3 minutes.' % queries
        # Time out for 3 minutes
        time.sleep(60*3)
        print 'Continuing execution'

        if not found_results:
            print 'Completed querying all tweets for the day: %s' % current_date

            # Move on to next day if all tweets have been processed for the current day.
            current_date += timedelta(days = -1)

            print "Current date: %s" % current_date

    print "Finished running all queries!"

# Read tweets from S3, write CSV files with word frequencies, and plot top 30 words per category
def process_tweets():
    # Required only if nltk packages haven't been downloaded to this computer
    # nltk.download()

    # Initialize list that will store [count of tweets, tokens] for each hashtag combination
    both_tags_info = [0, []]
    finals_tag_info = [0, []]
    warriors_tag_info = [0, []]

    # Connect to S3 and get all items in bucket.
    bucket = Utils.connect_to_S3()
    rs = bucket.list()

    # Get all JSON files we stored in S3
    json_files = []
    for key in rs:
        if key.name.endswith('.json'):
            json_files += [key.name]

    # Loop through all JSON files in S3
    for file in json_files:
        print 'Reading S3 file: %s' % file
        key = bucket.get_key(file)
        input = pd.read_json(key.get_contents_as_string())

        # Get all tweets with both hashtags and update the info we have on them
        df = input[input['HasBothHashTag']]['text'].values
        both_tags_info[0] += len(df)
        both_tags_info[1] += Utils.tokenize(df)

        # Get all tweets with the #NBAFinals2015 hashtag and update the info we have on them
        df = input[input['HasFinalsHashTag']]['text'].values
        finals_tag_info[0] += len(df)
        finals_tag_info[1] += Utils.tokenize(df)

        # Get all tweets with the #Warriors hashtag and update the info we have on them
        df = input[input['HasWarriorsHashTag']]['text'].values
        warriors_tag_info[0] += len(df)
        warriors_tag_info[1] += Utils.tokenize(df)

    # Print general info on each of the hashtag combinations
    print 'Both hashtags          - Tweets: %s, Tokens: %s' % (both_tags_info[0], len(both_tags_info[1]))
    print '#NBAFinals2015 hashtag - Tweets: %s, Tokens: %s' % (finals_tag_info[0], len(finals_tag_info[1]))
    print '#Warriors hashtag      -: %s, Tokens: %s' % (warriors_tag_info[0], len(warriors_tag_info[1]))

    # After all the tokens have been acquired get a histogram for each one
    both_tags_text = nltk.Text(both_tags_info[1])
    finals_tag_text = nltk.Text(finals_tag_info[1])
    warriors_tag_text = nltk.Text(warriors_tag_info[1])

    # Get frequency count for all tokens inside tweets that had both hashtags
    fd1 = nltk.FreqDist(both_tags_text)
    Utils.write_counts_to_csv(fd1, 'out/both_dist.csv')

    # Get frequency count for all tokens inside tweets that had the #NBAFinals2015 hashtag
    fd2 = nltk.FreqDist(finals_tag_text)
    Utils.write_counts_to_csv(fd2, 'out/finals_dist.csv')

    # Get frequency count for all tokens inside tweets that had the #Warriors hashtag
    fd3 = nltk.FreqDist(warriors_tag_text)
    Utils.write_counts_to_csv(fd3, 'out/warriors_dist.csv')

    # Plot histograms
    print 'Plotting frequency count graphs'
    fd1.plot(30)
    fd2.plot(30)
    fd3.plot(30)

    print 'Done'

if __name__ == '__main__':
    # Get tweets containing #NBAFinals2015 or #Warriors, and stores them in S3.
    get_tweets()

    # Read tweets from S3, write CSV files with word frequencies, and plot top 30 words per category
    process_tweets()

