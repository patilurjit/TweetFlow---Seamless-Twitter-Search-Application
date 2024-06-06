# -*- coding: utf-8 -*-
"""

@author: Atharva, Urjit & Nikhil

This script contains all the steps executed on corona-out-3 dataset provided.

"""

import json
import MySQLdb
import pandas as pd
from pymongo import MongoClient
from implementing_cache import Cache
from bson import json_util, Int64
from time import time

###############################################################################
## Functions
###############################################################################


# define function to create a JSON file from a given file path
# and write the data to a specified output file
def make_json(filepath, output_filename):
    """
    This function reads json data from a file, separates json objects from the file content,
    and saves the resulting json data to a new file.

    Parameters:
    filepath (str): The path of the input file containing json data.
    output_filename (str): The name of the output file to be created with the resulting json data.

    Returns:
    None
    """

    # open the input file and read its contents into a variable
    with open(filepath, "r") as f:
        json_data = f.read()

    # create a list to store each JSON object in the file
    json_objects = []

    # keep track of the number of braces encountered
    brace_count = 0

    # keep track of the index of the start of each JSON object
    start_index = None

    # loop through each character in the JSON data string
    for i, c in enumerate(json_data):
        # increment the brace count when an opening brace is encountered
        if c == "{":
            brace_count += 1

            # record the index of the opening brace if this is the first one encountered
            if brace_count == 1:
                start_index = i

        # decrement the brace count when a closing brace is encountered
        elif c == "}":
            brace_count -= 1

            # if this is the closing brace of the current JSON object,
            # append the JSON object to the list of JSON objects
            if brace_count == 0:
                json_objects.append(json_data[start_index : i + 1])

    # join the JSON objects into a single string separated by commas
    json_data = ",".join(json_objects)

    # parse the JSON data string into a list of dictionaries
    data = json.loads("[" + json_data + "]")

    # open the output file and write the JSON data to it
    with open(output_filename, "w") as f:
        json.dump(data, f)


# call the make_json function with the input file path and output file path as arguments
make_json("corona-out-3", "./json_files/corona-out-3.json")

# read the JSON data from the output file into a variable
with open("./json_files/corona-out-3.json", "r") as f:
    data = json.loads(f.read())

# connect to a MySQL database using the MySQLdb module
db = MySQLdb.connect(host="localhost", user="root", passwd="root", db="twitter_db")

# create a cursor object to interact with the database
cur = db.cursor()

###############################################################################
####### Creating tables for tweets, retweets, quoted_tweets
###############################################################################

# define SQL query to create a table called "user_data"
create_table_query = "CREATE TABLE IF NOT EXISTS user_data(\
	user_id BIGINT PRIMARY KEY NOT NULL,\
    user_id_str VARCHAR(255),\
    username VARCHAR(255),\
    full_name VARCHAR(255),\
    verfied BOOLEAN,\
    bio TEXT,\
    location VARCHAR(255),\
    url TEXT(255),\
    created_at TIMESTAMP,\
    followers_count INTEGER,\
    following_count INTEGER,\
    likes_count INTEGER,\
    total_tweets INTEGER,\
    lang VARCHAR(10),\
    INDEX(username)\
);"

# execute the SQL query to create the "user_data" table
cur.execute(create_table_query)


# define SQL query to create a table called "retweet_data"
create_table_query = "CREATE TABLE IF NOT EXISTS retweet_data(\
	retweet_id BIGINT PRIMARY KEY,\
    tweet_id VARCHAR(255),\
    user_id BIGINT,\
    created_at TIMESTAMP,\
    FOREIGN KEY (user_id) REFERENCES user_data(user_id)\
);"

# execute the SQL query to create the "retweet_data" table
cur.execute(create_table_query)


# define SQL query to create a table called "quoted_tweet_data"
create_table_query = "CREATE TABLE IF NOT EXISTS quoted_tweet_data(\
	quoted_tweets_id BIGINT PRIMARY KEY,\
    tweet_id VARCHAR(255),\
    user_id BIGINT,\
    created_at TIMESTAMP,\
    FOREIGN KEY (user_id) REFERENCES user_data(user_id)\
);"

# execute the SQL query to create the "quoted_tweet_data" table
cur.execute(create_table_query)

########################################################################################################################
############ This script extracts user account information from a Twitter data list and stores it in a MySQL database.
############ It also creates dictionaries to store the user information for retweets and quoted tweets separately.
########################################################################################################################

# list of keys to be extracted from data dictionary
key_list = [
    "id",
    "id_str",
    "screen_name",
    "name",
    "verified",
    "description",
    "location",
    "url",
    "created_at",
    "followers_count",
    "friends_count",
    "favourites_count",
    "statuses_count",
    "lang",
]

# SQL query for inserting data into user_data table
query_insert = "INSERT INTO user_data VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
%s,%s,%s);"

# dictionaries to hold extracted values
val_dict = {}
rt_val_dict = {}
qt_val_dict = {}

# loop through the data and extract the necessary values for each user
for i in range(len(data)):
    val_list = []

    # loop through each key and extract the corresponding value
    for key in key_list:
        if key == "created_at":
            # convert the created_at field to a datetime object
            data[i]["user"][key] = pd.to_datetime(data[i]["user"][key])
        val_list.append(data[i]["user"][key])

    # store the extracted values as a tuple in the val_dict
    val_dict[i] = tuple(val_list)

    # execute the SQL query with the extracted values
    try:
        cur.execute(query_insert, val_list)
    except Exception as e:
        print(e)
        pass

    # if the tweet is a retweet, extract the necessary values from the retweeted_status field
    if "retweeted_status" in data[i]:
        rt_val_list = []
        for key in key_list:
            if key == "created_at":
                # convert the created_at field to a datetime object
                data[i]["retweeted_status"]["user"][key] = pd.to_datetime(
                    data[i]["retweeted_status"]["user"][key]
                )
            rt_val_list.append(data[i]["retweeted_status"]["user"][key])
    else:
        continue

    # store the extracted values as a tuple in the rt_val_dict
    rt_val_dict[i] = tuple(rt_val_list)

    # execute the SQL query with the extracted values
    try:
        cur.execute(query_insert, rt_val_list)
    except Exception as e:
        print(e)
        pass

    # if the tweet is a quote tweet, extract the necessary values from the quoted_status field
    if "quoted_status" in data[i]:
        qt_val_list = []
        for key in key_list:
            if key == "created_at":
                # convert the created_at field to a datetime object
                data[i]["quoted_status"]["user"][key] = pd.to_datetime(
                    data[i]["quoted_status"]["user"][key]
                )
            qt_val_list.append(data[i]["quoted_status"]["user"][key])
    else:
        continue

    # store the extracted values as a tuple in the qt_val_dict
    qt_val_dict[i] = tuple(qt_val_list)

    # execute the SQL query with the extracted values
    try:
        cur.execute(query_insert, qt_val_list)
    except Exception as e:
        print(e)
        pass

# commit the changes to the database
db.commit()


###################################################################################################################
############ This script extracts retweet information from a Twitter data list and stores it in a MySQL database. 
############ It creates a dictionary to store the retweet information for each retweet.
###################################################################################################################


# Define a SQL query to insert data into a table called retweet_data
query_insert = "INSERT INTO retweet_data VALUES(%s,%s,%s,%s);"

# Create an empty dictionary to store the values for each tweet
val_dict = {}

# Loop through each tweet in the 'data' list
for i in range(len(data)):
    # Check if the tweet is a retweet
    if "retweeted_status" in data[i]:
        # Create a list to store the values for the retweet
        val_list = []

        # Append the tweet ID, retweet ID, user ID, and creation date to the list
        val_list.append(data[i]["id"])
        val_list.append(data[i]["retweeted_status"]["id"])
        val_list.append(data[i]["user"]["id"])
        val_list.append(pd.to_datetime(data[i]["created_at"]))

        # Store the list of values for the retweet in the dictionary
        val_dict[i] = tuple(val_list)

        # Try to execute the SQL query with the list of values
        try:
            cur.execute(query_insert, val_list)

        # If there's an error executing the query, print the error message and continue to the next tweet
        except Exception as e:
            print(e)
            pass

    # If the tweet is not a retweet, continue to the next tweet
    else:
        continue

# Commit the changes to the database
db.commit()

###########################################################################################################
########This script inserts data into a 'quoted_tweets' table in a SQL database. It checks if each element of a given 'data' list
########contains a 'quoted_status' key, and if so, creates a row with four values (tweet ID, quoted tweet ID, user ID, and created time of the quoted tweet).
########It then executes an SQL query to insert the values into the database, and prints the number of rows that were successfully inserted.
###########################################################################################################

# Define a SQL query for inserting data into a table called "quoted_tweet_data"
query_insert = "INSERT INTO quoted_tweet_data VALUES(%s,%s,%s,%s)"

# Create an empty dictionary to store values for each row to be inserted
val_dict = {}

# Iterate over the list "data"
for i in range(len(data)):
    # Check if the current element in "data" has a "quoted_status" key
    if "quoted_status" in data[i]:
        # Create an empty list to store values for the current row to be inserted
        val_list = []

        # Append values to "val_list" based on keys in "data" and "quoted_status"
        val_list.append(data[i]["id"])  # tweet ID
        val_list.append(data[i]["quoted_status"]["id"])  # quoted tweet ID
        val_list.append(data[i]["user"]["id"])  # user ID
        val_list.append(
            pd.to_datetime(data[i]["quoted_status"]["created_at"])
        )  # creation time of quoted tweet

        # Store values for the current row as a tuple in "val_dict"
        val_dict[i] = tuple(val_list)

        # Try to execute the SQL query to insert values for the current row
        try:
            cur.execute(query_insert, val_list)

        # If an error occurs, print the error message and continue to the next row
        except Exception as e:
            print(e)
            pass

    # If the current element in "data" does not have a "quoted_status" key, skip it and move on to the next row
    else:
        continue

# Commit changes to the database
db.commit()

cur.close()
db.close()
###############################################################################
########## This script extracts data from a JSON file containing tweet data, and inserts the data into a MongoDB database.
########## It loops through each tweet in the data object and extracts the tweet's ID, text, created time, source, user ID, language, 
########## and popularity score (the sum of the counts for quote, reply, retweet, and favorite). The extracted data is then inserted 
########## into the 'tweets_data' collection in the 'trial' database in MongoDB.
###############################################################################

# Try to establish a connection to MongoDB
try:
    conn = MongoClient()
    print("Connected successfully")
except:
    print("Could not connect to MongoDB")

# Connect to the 'twitter_db' database and the 'tweets_data' collection
db = conn.twitter_db
collection = db.tweets_data

# Define the keys that will be used to extract data from the JSON object
keys = [
    "id",
    "id_str",
    "text",
    "created_at",
    "is_quote_status",
    "quote_count",
    "reply_count",
    "entities",
    "retweet_count",
    "favorite_count",
    "lang",
    "timestamp_ms",
    "geo",
]


# Define a function to extract the source of a tweet from its source string
def extract_source(input_string):
    """
    This function extracts the source of a tweet from its source string.

    Parameters:
    input_string (str): The source string of the tweet.

    Returns:
    str: The source of the tweet (either 'iPhone', 'Android', 'WebApp', or 'Instagram').
    If the source is not found, returns None.
    """

    sources = ["iPhone", "Android", "WebApp", "Instagram"]

    for source in sources:
        if source in input_string:
            extracted_source = source
            return extracted_source


# Define a function to insert a tweet into MongoDB
def mongo_insertor(index, keys):
    """
    This function takes a JSON object representing a tweet and inserts it into a MongoDB collection.
    It extracts the specified keys from the JSON object and adds them to a dictionary, along with
    other relevant information such as the tweet's source, user ID, and popularity score.

    Parameters:
    index (dict): A JSON object representing a tweet.
    keys (list): A list of keys to extract from the tweet JSON object.

    Returns:
    dict: A dictionary containing the relevant information extracted from the tweet JSON object.
    """

    obj = {"_id": Int64(index["id"]), "source": extract_source(index["source"])}

    for key in keys:
        try:
            obj[key] = index[key]
        except:
            pass

    if "extended_tweet" in index.keys():
        obj["text"] = index["extended_tweet"]["full_text"]

    obj["user_id"] = Int64(index["user"]["id"])

    obj["popularity"] = (
        index["quote_count"]
        + index["reply_count"]
        + index["retweet_count"]
        + index["favorite_count"]
    )
    return obj


# Loop through each item in the data list
for index in data:
    # Check if the current item has a "retweeted_status" key
    if "retweeted_status" in index.keys():
        # If it does, create a new object from the "retweeted_status" key using the "mongo_insertor" function
        obj = mongo_insertor(index["retweeted_status"], keys)
        # Attempt to insert the new object into the collection using the "insert_one" method
        try:
            collection.insert_one(obj)
        # If an exception is raised during insertion, print the error message and continue to the next item
        except Exception as e:
            print(e)
            pass

    # Check if the current item has a "quoted_status" key
    if "quoted_status" in index.keys():
        # If it does, create a new object from the "quoted_status" key using the "mongo_insertor" function
        obj = mongo_insertor(index["quoted_status"], keys)
        # Attempt to insert the new object into the collection using the "insert_one" method
        try:
            collection.insert_one(obj)
        # If an exception is raised during insertion, print the error message and continue to the next item
        except Exception as e:
            print(e)
            pass

    # Create a new object from the current item using the "mongo_insertor" function
    obj = mongo_insertor(index, keys)
    # Attempt to insert the new object into the collection using the "insert_one" method
    try:
        collection.insert_one(obj)
    # If an exception is raised during insertion, print the error message and continue to the next item
    except Exception as e:
        print(e)
        pass

# Create an index on the "user_id" field of the collection using the "create_index" method
collection.create_index("user_id")

conn.close()