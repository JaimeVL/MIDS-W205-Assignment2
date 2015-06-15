import sys
from datetime import date, timedelta
import tweepy
import json
import signal
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class TweetSerializer:
    out = None
    first = True
    count = 0
    json_contents = []
    lowest_id = None
    lowest_datetime = None
   
    def start(self):
        self.count += 1
        fname = "tweets-"+str(self.count)+".json"
        self.out = open(fname,"w")
        self.out.write("[\n")
        self.first = True

    def end(self):
        if self.out is not None:
            self.out.write("\n]\n")
            self.out.close()
        self.out = None

        print self.lowest_id
        print self.lowest_datetime

    def write(self,tweet):
        json_data = { key: tweet._json[key] for key in ['created_at', 'lang', 'text', 'id'] }
        json_data['screen_name'] = tweet._json['user']['screen_name']

        current_lowest_datetime = json_data['created_at']
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

            self.out.write(",\n")

        # Remove later, can be calculated afterwards...
        text = json_data['text'].lower()
        json_data['HasFinalsHashTag'] = ('#nbafinals2015' in text)
        json_data['HasWarriorsHashTag'] = ('#warriors' in text)

        self.first = False

        self.json_contents += [json_data]
        #self.out.write(json.dumps(json_data, indent=2, separators=(',', ': ')).encode('utf8'))

def interrupt(signum, frame):
   print "Interrupted, closing ..."
   if ts.out is not None:
      print "End manually"
      ts.end()
   # magic goes here
   exit(1)

def connect_to_S3():
    conn = S3Connection('AKIAJUY25MMC4TJML2GA', 'GC+5D+FmGzXxeLzgwpfiDWJqMECuksMdGkI3yGb1')
    return conn.get_bucket('jvl-mids-w205-assignment2')

def connect_to_Twitter():
    consumer_key = "QzphhBGR9WZvmxF9a17OalniM";
    consumer_secret = "PW6ZM1ZsTKwpoTnL6YqkTKFROpAZ1CNbhwjbXM62PfSSsQxU5G";
    access_token = "319832087-u0NYYcL6Sj73RwOgcaPMAxHlyPrlDyl02o3Q2ROK";
    access_token_secret = "cAHHOJo12y18jIIuRapXyJHP2AcOA1oxJqs4rtjwhqg2S";

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    return tweepy.API(auth_handler=auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

def query_Twitter(api, ts, date, max_id = 0):
    nextDate = date + timedelta(days = 1)
    query = '#NBAFinals2015 OR #Warriors since:' + date.strftime('%Y-%m-%d') + " until:" + nextDate.strftime('%Y-%m-%d') + ((' max_id:%s' % max_id) if max_id > 0 else '')
    # query = "#NBAFinals2015 OR #Warriors since:2015-06-12 until:2015-06-13 max_id:609510488829853696"

    count = 0
    for tweet in tweepy.Cursor(api.search, q = query).items(2):
        count += 1
        ts.write(tweet)

    print "Done. Count: %s" % count

def main():
    bucket = connect_to_S3()
    api = connect_to_Twitter()
    signal.signal(signal.SIGINT, interrupt) # Used to interrupt execution

    ts = TweetSerializer()
    ts.start()

    # Set start, end, and current dates used to track progress of work.
    start_date = date(2015,06,11)
    end_date = date(2015,06,18)
    current_date = start_date

    # Process data one day at a time
    while current_date < date.today() and current_date < end_date:
        print "Current date: %s" % current_date

        # 1) Get initial tweets (no max_id used)
        query_Twitter(api, ts, current_date)
        ts.end()
        return
        # 2) Process tweets and get: lowest Id, lowest datetime, hasFinalsHashTag, hasWarriorsHashTag, fileName
        # 3) Write to local JSON file
        # 4)

        # Move on to next day if all tweets have been processed for the current day.
        current_date += timedelta(days = 1)

    print "failed"

    '''
    #key = Key(bucket)
    #key.key = 'myfile.txt'

    key = bucket.new_key('2015/06/tweets-1.json')
    # Store content to S3
    key.set_contents_from_filename('tweets-1.json')
    # Get content stored in S3
    #key.get_contents_to_filename('test.txt')

    print "test"
    return

    # Accessing a bucket
    myBucket = conn.get_bucket('mybuck-w205-2014')
    for key in myBucket.list():
        print key.name.encode('utf-8')
    '''

    ###################
    # Twitter section #
    ###################


    query = "#NBAFinals2015 OR #Warriors since:2015-06-12 until:2015-06-13 max_id:609510488829853696"

    count = 1
    #for tweet in tweepy.Cursor(api.search,q = q + " since:2015-06-10 until:2015-05-12").items(10):
    for tweet in tweepy.Cursor(api.search, q = query).items(10):
        if count % 10 == 0:
            print count
        count += 1
        ts.write(tweet)

    print "Done. Count: %s" % count
    ts.end()

if __name__ == '__main__':
    main()