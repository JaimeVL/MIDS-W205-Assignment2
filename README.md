# MIDS W205 Assignment#2 - Jaime Villlapando #

I collected Twitter data between 2015-06-11 and 2015-06-17 (inclusive). Almost 300K tweets were collected and processed. Below I will describe in more detail the design decisions I took in this assignment.

## Details on Data Acquisition Design ##

The task was divided in two sections:

### Get tweets and store them in S3 ###

1. Start with 2015-06-17 as the *current_date* value.
2. Query for 1000 tweets at a time containing #NBAFinals2015 or #Warriors hashtags. After each query, the lowest **id** value is stored and then the **max_id** query parameter is used in the very next query so that different tweets would be obtained. Here were all the query parameters used:
  a. query = '#NBAFinals2015 OR #Warriors since:*current_date* until:*current_date*
  b. max_id = *lowest_id - 1*
3. Each results is stored in a dictionary containing the following values: **created_at**, **lang**, **text**, **id**, and **screen_nane**. Along with these, three new self-explanatory values were added to facilitate consumption in section 2: 
  a. **HasFinalsHashTag**
  b. **HasWarriorsHashTag**
  c. **HasBothHashTag**
4. A list containing each tweet keeps growing. Subsequent queries are made (back to step 2) until more than 5000 tweets have been processed. Afterwards, the tweets are stored in a JSON file which is uploaded to S3 (the local file is deleted afterwards).   
5. Go back to step 1, but with a *current_date* value of the previous day. Do this until tweets from 2015-06-11 have been processed.

### Read tweets from S3, write CSV files, and plot top 30 words per category ###

1. Iterate through all the files stored in the S3 bucket.
2. For each file, the contents are read and inserted into a dataframe. Then only the entries which match each category (e.g. **Only #NBAFinals2015**, **Only #Warriors**, **Both hashtags**) are processed separately as described in step 3. 
3. For each category, the tweets are tokenized using the following rules: 
  a. Each word is separated by punctuation marks or spaces.
  b. All words are converted to lower case.
  c. URLs, stopwords, and the #NBAFinals2015 and #Warriors hashtags are discarded.
4. After all files have been processed, the frequency counts are computed and the CSV files for each category are written to disk.
5. Finally, for each category, a plot is shown. A png image is saved from the output of each.
 
## Deliverables ##
 
1. Here's is the S3 bucket in which all my output files are stored: jvl-mids-w205-assignment2.
2. My twitter acquisition code is located in the main.py file.
3. For each category (tweets with only #NBAFinals2015, #Warriors, or both) I've included both a CSV file with the frequency of all words encountered (minus stopwords, URLs, and those two hashtags) as well as a plot showing the frequencies of the top 30 words. Here's 
  a. **Only #NBAFinals2015** - finals_dist.csv and finals_plot.png
  ![alt text](/out/finals_plot.png "#NBAFinals2015")
  b. **Only #Warriors** - warriors_dist.csv and warriors_plot.png
  ![alt text](/out/warriors_plot.png "#Warriors")
  c. **Both hashtags** - both_dist.csv and both_plot.png
  ![alt text](/out/both_plot.png "Both HashTags")