#This script creates a users data file from the streamed raw data twitter data.
#This users file is used to spawn crawlers to fetch historical data

import os
import tweepy
import json
from json import JSONDecodeError
from tweepy import TweepError
import time
import csv

CONSUMER_KEY = "azASVSfOtpqplTTxJPq0pAy7d"
CONSUMER_SECRET = "cjvhl4fWWCrioCk2RMaOMWyPWqSo28deA66cjzj7EjXq04k49e"

ACCESS_TOKEN = "555865355-ATv0cnFGW7NVjb3n8XTTe7e8gkomW7CVoLEeZEM9"
ACCESS_TOKEN_SECRET = "TNZWkuSxH1MVYgKWSZMc9M1imLuKKuEdVUTg4ej6gfN6S"

STREAMED_DATA_FOLDER = "Streamed_Data"
REF_FILES_FOLDER = "Ref_files"
USER_DATA_FILE = "users.csv"
STREAM_STATS_FILE = "stream_stats.txt"
current_file = os.path.abspath(os.path.dirname(__file__))
streamed_data_path = os.path.join(current_file, STREAMED_DATA_FOLDER)
user_data_file = os.path.join(current_file, USER_DATA_FILE)
stream_stats_file = os.path.join(current_file, STREAM_STATS_FILE)
ref_file_path = os.path.join(current_file, REF_FILES_FOLDER)
NO_DATA_STRING = "NO_DATA"


state_set = set()
abbr_state_set = set()
county_set = set()
cities_set = set()

def getUserDetails(userObj):
    user_id = user_name = user_location = user_tz = NO_DATA_STRING
    if userObj['id'] is not None:
        user_id = userObj['id']
    if userObj['screen_name'] is not None:
        user_name = userObj['screen_name'].strip()
    if userObj['location'] is not None:
        user_location = userObj['location'].strip()
    if userObj['time_zone'] is not None:
        user_tz = userObj['time_zone'].strip()
    return user_id, user_name, user_location, user_tz

def createUsersFile(userDict):
    user_loc_not_found_count = 0
    with open(user_data_file, 'w') as user_file:
        wr = csv.writer(user_file, quoting=csv.QUOTE_ALL)
        wr.writerow(["User_ID", "User_Name", "User_Location", "User_Timezone", "IS_USA_USER", "USER_CITY", "COUNTY", "STATE", "COUNTRY"])
        for key, value in user_dict.items():
            if value[2] == NO_DATA_STRING and value[3] == NO_DATA_STRING:
                user_loc_not_found_count += 1
            wr.writerow(value)
    return user_loc_not_found_count

def getProcessedLocation(loc, tz):
    is_usa_user = user_city = user_county = user_state = user_country = NO_DATA_STRING
    other_tokens = ["usa", "united states", "united states of america", "us"]

    tokens = loc.split(",")
    for word in tokens:
        token = word.lower().strip()
        if user_city == NO_DATA_STRING and token in cities_set:
            user_city = token
        if user_county == NO_DATA_STRING and token in county_set:
            user_county = token
        if user_state == NO_DATA_STRING and token in state_set:
            user_state = token
        if user_state == NO_DATA_STRING and token in abbr_state_set:
            user_state = token
        if user_country == NO_DATA_STRING and token in other_tokens:
            user_country = token
    if user_city == NO_DATA_STRING and user_state == NO_DATA_STRING and user_county == NO_DATA_STRING and user_country == NO_DATA_STRING:
        is_usa_user = "NO"
    else:
        is_usa_user = "YES"

    us_tzs = ["Pacific Time (US & Canada)", "Eastern Time (US & Canada)", "Central Time (US & Canada)"]
    if is_usa_user == "NO" and tz.strip() in us_tzs:
        is_usa_user = "YES"

    return is_usa_user, user_city, user_county, user_state, user_country


if __name__ == "__main__":
    start_time = time.time()
    auth = tweepy.OAuthHandler(consumer_key=CONSUMER_KEY, consumer_secret=CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    directory = os.listdir(streamed_data_path)
    os.chdir(streamed_data_path)

    decode_error_count = 0
    user_obj_not_found_count = 0
    total_tweets_streamed = 0
    us_users_count = 0
    us_users_without_loc = 0


    user_dict = dict()

    glc_fn = os.path.join(ref_file_path, "GLC.csv")
    cities_fn = os.path.join(ref_file_path, "us_cities_states_counties.csv")

    #Create US cities, states, counties map
    with open(glc_fn, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            state_set.add(row[1].lower().strip())
            abbr_state_set.add(row[2].lower().strip())
            county_set.add(row[4].lower().strip())
    with open(cities_fn, 'r') as f:
        reader = csv.reader(f)
        for line in reader:
            row = line[0].split("|")
            try:
                cities_set.add(row[0].lower().strip())
                cities_set.add(row[4].lower().strip())
                abbr_state_set.add(row[1].lower().strip())
                state_set.add(row[2].lower().strip())
                county_set.add(row[3].lower().strip())
            except IndexError as err:
                pass

    print("Creating Users File...")
    with open(user_data_file, 'r') as user_file:
        reader = csv.reader(user_file)
        for row in reader:
            if row[4] == "YES":
                user_dict[row[0]] = ["OLD"]

    print("Users:", len(user_dict))
    new_users_found = 0
    try:
        for page in tweepy.Cursor(api.followers_ids, screen_name="BarackObama").pages(100):
            for user_id in page:
                if user_id not in user_dict:
                    user_dict[user_id] = ["NEW", "ID"]
                    new_users_found += 1
            print("New users found= ", new_users_found)
            new_users_found = 0
            print("sleeping for 1 minute")
            time.sleep(60 * 1)
    except TweepError as err:
        print(err)
    print("Users Latest:", len(user_dict))

    temp_user_ids_fn = os.path.join(current_file, "temp_user_ids")
    with open(temp_user_ids_fn, 'w') as temp_file:
        for user_id in user_dict.keys():
            temp_file.write(str(user_id) + "\n")



    # no_loc_users_count = createUsersFile(user_dict)
    # with open(stream_stats_file, 'w') as st_file:
    #     st_file.write("Decode Error Count = " + str(decode_error_count) + "\n")
    #     st_file.write("User not found = " + str(user_obj_not_found_count) + "\n")
    #     st_file.write("Total Tweets Streamed = " + str(total_tweets_streamed) + "\n")
    #     st_file.write("Unique Users = " + str(len(user_dict)) + "\n")
    #     st_file.write("User without location info = " + str(no_loc_users_count) + "\n")
    #     st_file.write("US User Count = " + str(us_users_count) + "\n")
    #     st_file.write("US User without proper location = " + str(us_users_without_loc) + "\n")
    print("Users File Created.")
    print("--- %s seconds ---" % (time.time() - start_time))

