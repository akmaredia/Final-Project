import twitter_inf
import tweepy
import json
import requests
import re
import unittest
import sqlite3
import collections

consumer_key = twitter_info.consumer_key
consumer_secret = twitter_info.consumer_secret
access_token = twitter_info.access_token
access_token_secret = twitter_info.access_token_secret

auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)

api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

class Movie(object):

    def __init__(self, movie):
        self.title = movie["Title"]
        self.director = movie["Director"]
        self.rating = movie["imdbRating"]
        self.actors = movie["Actors"]
        self.language = movie["Language"]
        self.id = movie["imdbID"]

    def __str__(self):
        return "The title of the movie is " + self.title

    def top_actor(self):
        a = self.actors.split(",")
        return a[0]

    def languages(self):
        count = 0
        s = self.language.split(",")
        for l in s:
            count+=1
        return int(count)

def twitter_search(phrase):

    twitter_data = "cached_data_twitter_search.json"
    try:
        twitter_file = open(twitter_data, 'r')
        twitter_contents = twitter_file.read()
        twitter_file.close()
        TWITTER_DICT = json.loads(twitter_contents)
    except:
        TWITTER_DICT = {}

    unique_identifier = "twitter_{}".format(phrase)
    if unique_identifier in TWITTER_DICT:
        print('using cached data for', phrase)
        twitter_results = TWITTER_DICT[ unique_identifier ]
        return twitter_results
    else:
        print('getting data from internet for', phrase)
        twitter_results = api.search(phrase)
        TWITTER_DICT[ unique_identifier ] = twitter_results
        f = open(twitter_data, 'w')
        f.write(json.dumps(TWITTER_DICT))
        f.close()
        return twitter_results

def twitter_user(username):

    username_data = "cached_data_twitter_user.json"
    try:
        username_file = open(username_data, 'r')
        username_contents = username_file.read()
        username_file.close()
        USERNAME_DICT = json.loads(username_contents)
    except:
        USERNAME_DICT = {}

    unique_id = "twitter_{}".format(username)
    if unique_id in USERNAME_DICT:
        print("using cached data for", username)
        username_results = USERNAME_DICT[ unique_id ]

    else:
        print("getting data from internet for", username)
        username_results = api.get_user(username)
        USERNAME_DICT[ unique_id ] = username_results

        f = open(username_data, 'w')
        f.write(json.dumps(USERNAME_DICT))
        f.close()

    return username_results

def get_movie_data(title):

    movie_data = "cached_data_movie.json"

    try:
        movie_file = open(movie_data, 'r')
        movie_contents = movie_file.read()
        MOVIE_DICT = json.loads(movie_contents)
    except:
        MOVIE_DICT = {}

    if title in MOVIE_DICT:
        print("using cached data for", title)
        movie = MOVIE_DICT[title]

    else:
        print("getting data from internet for", title)
        omdb = requests.get('http://www.omdbapi.com/?t=' + title).text
        movie = json.loads(omdb)
        MOVIE_DICT[title] = movie

        f = open(movie_data, 'w')
        f.write(json.dumps(MOVIE_DICT))
        f.close()

    return movie

def main():
    l = ["Shawshank+Redemption", "Fargo", "Django+Unchained"]

    movie_list = []
    for m in l:
        movie_list.append(get_movie_data(m))

    movie_class_list = []
    for c in movie_list:
        movie_class_list.append(Movie(c))

    tweet_list = []
    for movie in movie_class_list:
        tweet_list.append(twitter_search(movie.top_actor()))

    tweet_list2 = []
    for t in tweet_list:
        search = t["search_metadata"]
        actor = search["query"]
        a = actor.replace("+", " ")
        status = t["statuses"]
        for s in status:
            d = {}
            d["actor"] = a
            d[ "status" ] = s[ "text" ]
            d["tweet_id"] = s["id"]
            d[ "favorites" ] = int(s[ "favorite_count" ])
            d[ "retweets" ] = int(s[ "retweet_count" ])
            user = s[ "user" ]
            d[ "username" ] = user[ "screen_name" ]
            tweet_list2.append(d)

    users = []
    for name in tweet_list2:
        if name["username"] not in users:
            users.append(name["username"])

    users2 = []
    for u in users:
        users2.append(twitter_user(u))

    users_info = []
    for u in users2:
        info = {}
        info["favorites_ever"] = u["favourites_count"]
        info["id"] = u["id"]
        info["user_name"] = u["screen_name"]
        info["location"] = u["time_zone"]
        info["language"] = u["lang"]
        users_info.append(info)

    connection = sqlite3.connect("project.db")
    database_cursor = connection.cursor()

    database_cursor.execute("DROP TABLE IF EXISTS Tweets")
    database_cursor.execute("DROP TABLE IF EXISTS Users")
    database_cursor.execute("DROP TABLE IF EXISTS Movies")

    database_cursor.execute("CREATE TABLE IF NOT EXISTS Tweets (tweet_id INTEGER PRIMARY KEY, tweet TEXT, user_name TEXT, movie_actor TEXT, favorites INTEGER, retweets INTEGER)")
    database_cursor.execute("CREATE TABLE IF NOT EXISTS Users (user_id INTEGER PRIMARY KEY, user_name TEXT, favorites_ever INTEGER, location TEXT, language TEXT)")
    database_cursor.execute("CREATE TABLE IF NOT EXISTS Movies (movie_id TEXT PRIMARY KEY, title TEXT, director TEXT, languages INTEGER, imdb_rating FLOAT, top_actor TEXT)")


    insert_statement_tweets = "INSERT INTO Tweets VALUES (?, ?, ?, ?, ?, ?)"
    for tweet in tweet_list2:
        database_cursor.execute(insert_statement_tweets, (
        tweet[ "tweet_id" ], tweet[ "status" ], tweet[ "username" ], tweet["actor"], tweet[ 'favorites' ],
        tweet[ "retweets" ]))

    insert_statement_users = "INSERT INTO Users VALUES (?, ?, ?, ?, ?)"
    for user in users_info:
        database_cursor.execute(insert_statement_users, (
        user[ "id" ], user[ "user_name" ], user[ "favorites_ever" ], user["location"], user["language"]))

    insert_statement_movie = "INSERT INTO Movies VALUES (?, ?, ?, ?, ?, ?)"
    for m in movie_class_list:
        database_cursor.execute(insert_statement_movie, (
        m.id, m.title, m.director, m.languages(), float(m.rating), m.top_actor()))

    database_cursor.execute("SELECT Tweets.retweets, Movies.top_actor FROM Tweets INNER JOIN Movies ON Tweets.movie_actor = Movies.top_actor")
    result_set = database_cursor.fetchall()

    name_list = [n[1] for n in result_set]

    name_d = {}
    for name in name_list:
        count = 0
        for n in result_set:
            if n[1] == name:
                count += n[0]
                name_d[n[1]] = count

    sorted_actors = sorted(name_d.items(), key=lambda x: x[1], reverse = True)
    print(sorted_actors)

    connection.commit()

    database_cursor.execute("SELECT Tweets.movie_actor FROM Tweets INNER JOIN Users ON Tweets.user_name = Users.user_name WHERE Users.language = 'en'")
    result_set2 = database_cursor.fetchall()

    name_list2 = [n[0] for n in result_set2]

    count_movie_actors = collections.Counter()
    for actor in name_list2:
        count_movie_actors[actor] += 1

    actors_d = dict(count_movie_actors)
    print(actors_d)

    connection.commit()


    database_cursor.execute("SELECT Users.location, Movies.top_actor FROM Users INNER JOIN Tweets ON Users.user_name = Tweets.user_name INNER JOIN Movies ON Tweets.movie_actor = Movies.top_actor")
    result_set3 = database_cursor.fetchall()

    name_list3 = []
    for n in result_set3:
        if n[1] not in name_list3:
            name_list3.append(n[1])

    locations_d = {}
    for name in name_list3:
        count = 0
        for n in result_set3:
            if n[1] == name:
                if n[0] != None:
                    l_match = re.search(r'.(US & Canada)', n[0])
                    if l_match != None:
                        count += 1
                        locations_d[n[1]] = count
    print(locations_d)

    connection.commit()


    database_cursor.execute("SELECT Users.favorites_ever, Movies.top_actor FROM Users INNER JOIN Tweets ON Users.user_name = Tweets.user_name INNER JOIN Movies ON Tweets.movie_actor = Movies.top_actor")
    result_set4 = database_cursor.fetchall()

    name_list4 = [n[1] for n in result_set4]

    favorites_ever = {}
    for name in name_list4:
        count = 0
        for n in result_set4:
            if n[1] == name:
                count += n[0]
                favorites_ever[n[1]] = count
    print(favorites_ever)

    connection.commit()

    connection.close()

    file_object = open("project_data.txt", "w")
    file_object.write("Summary Twitter Stats for the Top Actors in Shawshank Redemption, Fargo, and Django Unchained by Aziz Maredia" + "\n")
    file_object.write("\n")
    file_object.write("Each top actor sorted by the most amount of retweeted tweets with their names in the tweet" + "\n")
    for t in sorted_actors:
        file_object.write(str(t[0]) + ":" + " " + str(t[1]) + "\n")
    file_object.write("\n")
    file_object.write("Number of tweets about each top actor where the twitter user speaks english" + "\n")
    for key, value in actors_d.items():
        file_object.write(str(key) + ":" + " " + str(value) + "\n")
    file_object.write("\n")
    file_object.write("Number of tweets about each top actor from the US or Canada" + "\n")
    for key, value in locations_d.items():
        file_object.write(str(key) + ":" + " " + str(value) + "\n")
    file_object.write("\n")
    file_object.write("Number of favorites ever from users that have tweeted about each top actor" + "\n")
    for key, value in favorites_ever.items():
        file_object.write(str(key) + ":" + " " + str(value) + "\n")

if __name__ == '__main__':
    main()

class TwitterSearchFuncTests(unittest.TestCase):
    
    def test_twitter(self):
        search = twitter_search("UMSI")
        self.assertEqual(type(search), type({}))
    
    def test_twitter2(self):
        fpt = open("cached_data_twitter_search.json", "r")
        fpt_str = fpt.read()
        fpt.close()
        obj = json.loads(fpt_str)
        self.assertEqual(type(obj), type({}))
    
class TwitterUsersFuncTests(unittest.TestCase):

    def test_user(self):
        search = twitter_search("AzizMaredia")
        self.assertEqual(type(search), type({}))
    
    def test_user2(self):
        fpt = open("cached_data_twitter_user.json", "r")
        fpt_str = fpt.read()
        fpt.close()
        obj = json.loads(fpt_str)
        self.assertEqual(type(obj), type({}))
    
class GetMovieDataFuncTests(unittest.TestCase):

    def test_movie(self):
        m = get_movie_data("Fargo")
        self.assertEqual(type(m), type({}))
    
    def test_movie2(self):
        fpt = open("cached_data_movie.json", "r")
        fpt_str = fpt.read()
        fpt.close()
        obj = json.loads(fpt_str)
        self.assertEqual(type(obj), type({}))
    
class MovieClassTests(unittest.TestCase):

    def test_constructor(self):
        m1 = Movie(get_movie_data("Fargo"))
        self.assertEqual(m1.title, "Fargo")

    def test_string(self):
        m2 = Movie(get_movie_data("Fargo"))
        self.assertEqual(m2.__str__(), "The title of the movie is Fargo")
    
    def test_top_actor(self):
        m3 = Movie(get_movie_data("Fargo"))
        self.assertEqual(m3.top_actor(), "William H. Macy")
    
    def test_languages(self):
        m4 = Movie(get_movie_data("Fargo"))
        self.assertEqual(m4.languages(), 1)
    
unittest.main(verbosity=2)
