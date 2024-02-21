"""
DS4300
Mia Huebscher and Launna Atkinson
MongoDB Tutorial with Congressional Tweet Data, Spring 2024

Constructs an API that can communicate with MongoDB to query for data regarding tweets made by US Congress members.
"""
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
import plotly.express as px
import plotly.io as pio
pio.renderers.default = "browser"


class congressionalTweets():
    def __init__(self):
        """
        Initializes a client to connect to MongoDB and....
        """
        self.client = MongoClient()
        self.congressTweets = self.client['congressTweets']
        self.users = self.congressTweets.users
        self.tweets = self.congressTweets.tweets

    def close_connection(self):
        """
        Closes the connection to Mongo
        :return: None
        """
        # Close the connection
        self.client.close()
        self.client = None

    def most_popular_tweets(self, n, popularity_metric, require_hashtags=False, earliest_date=None,
                            latest_date=None, return_data=None,):
        """
        Returns the most popular tweets (based on the given popularity metric) in the data.
        :param n: number of documents to retrieve
        :param popularity_metric: used to determine popularity (either retweet_count or favorite_count)
        :param require_hashtags: bool
        :param earliest_date: earliest creation date in data (format: %m/%d/%Y)
        :param latest_date: latest creation date in data (format: %m/%d/%Y)
        :param return_data: a list of data to return
        :return: a list of documents (dictionaries)
        """
        # Create a connection to the tweets collection
        tweets = self.tweets

        # Specify the query conditions and reflect any parameters given by the user
        query_data = {}
        if require_hashtags:
            query_data['entities.hashtags'] = {'$ne': []}
        if earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(earliest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at'] = {'$gte': timestamp}
        if latest_date and earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(latest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at']['$lte'] = timestamp
        elif latest_date and not earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(latest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at'] = {'$lte': timestamp}

        # Specify the return data based on if it's user given or not
        if return_data:
            return_data = dict(zip(return_data, [1 for _ in return_data]))
        else:
            return_data = {'coordinates': 1, 'created_at': 1, 'display_text_range': 1, 'entities.hashtags.text': 1,
                           'entities.user_mentions.screen_name': 1, 'favorite_count': 1, 'place': 1, 'retweet_count': 1,
                           'screen_name': 1, 'text': 1, 'user_id': 1}

        # Obtain the data from the database and return it to the user
        pop_tweets = tweets.find(query_data, return_data).sort({popularity_metric: -1}).limit(n)
        return list(pop_tweets)

    def get_user_tweets(self, username, n, sort="created_at", require_hashtags=False, return_data=None,
                        earliest_date=None, latest_date=None):
        """
        Returns the tweets made by a particular user.
        :param username: screen name of user to get data for
        :param n: number of documents to retrieve
        :param sort: field to sort the data by, default is creation date
        :param require_hashtags: bool
        :param return_data: a list of data to return
        :param earliest_date: earliest creation date in data (format: %m/%d/%Y)
        :param latest_date: latest creation date in data (format: %m/%d/%Y)
        :return: a list of documents (dictionaries)
        """
        # Create a connection to the tweets collection
        tweets = self.tweets

        # Specify the query conditions and reflect any parameters given by the user
        query_data = {'screen_name': username}
        if require_hashtags:
            query_data['entities.hashtags'] = {'$ne': []}
        if earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(earliest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at'] = {'$gte': timestamp}
        if latest_date and earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(latest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at']['$lte'] = timestamp
        elif latest_date and not earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(latest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at'] = {'$lte': timestamp}

        # Specify the return data based on if it's user given or not
        if return_data:
            return_data = dict(zip(return_data, [1 for _ in return_data]))
        else:
            return_data = {'coordinates': 1, 'created_at': 1, 'display_text_range': 1, 'entities.hashtags.text': 1,
                           'entities.user_mentions.screen_name': 1, 'favorite_count': 1, 'place': 1, 'retweet_count': 1,
                           'screen_name': 1, 'text': 1, 'user_ud': 1}

        # Obtain and return the users tweets
        user_tweets = tweets.find(query_data, return_data).sort({sort: -1}).limit(n)
        return list(user_tweets)

    def tweets_by_time(self, agg_by, earliest_date=None, latest_date=None):
        """
        Aggregates the tweets by time.
        :param agg_by: either 'year' or 'month'
        :param earliest_date: earliest creation date in data (format: %m/%d/%Y)
        :param latest_date: latest creation date in data (format: %m/%d/%Y)
        :return:
        """
        # Create a connection to the tweets collection
        tweets = self.tweets

        # Specify the match conditions and reflect any parameters given by the user
        query_data = {}
        if earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(earliest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at'] = {'$gte': timestamp}
        if latest_date and earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(latest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at']['$lte'] = timestamp
        elif latest_date and not earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(latest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at'] = {'$lte': timestamp}

        # Specify the parameters for the aggregation pipeline
        add_fields = {'$addFields': {'convertedDate': {'$dateFromString': {'dateString': {
            '$dateToString': {'format': '%Y-%m-%d', 'date': {'$toDate': {'$multiply': [1000, '$created_at']}}}},
            'format': '%Y-%m-%d'}}}}
        match = {'$match': query_data}
        group = {'$group': {'_id': {'$'+agg_by: '$convertedDate'}, 'Count': {'$sum': 1}}}
        sort = {'$sort': {'_id': -1}}

        # Execute the query
        data = tweets.aggregate([add_fields, match, group, sort])
        return list(data)

    def geography(self, color_on, earliest_date=None, latest_date=None):
        """
        Generates a plot of the United States and tweet locations.
        :param color_on: the field to differentiate the tweets by
        :param earliest_date: earliest creation date in data (format: %m/%d/%Y)
        :param latest_date: latest creation date in data (format: %m/%d/%Y)
        :return: None
        """
        # Create a connection to the tweets collection
        tweets = self.tweets

        # Specify the query conditions and reflect any parameters given by the user
        query_data = {'coordinates': {'$ne': None}}
        if earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(earliest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at'] = {'$gte': timestamp}
        if latest_date and earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(latest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at']['$lte'] = timestamp
        elif latest_date and not earliest_date:
            # Convert string date to timestamp
            str_datetime = datetime.strptime(latest_date, '%m/%d/%Y')
            timestamp = datetime.timestamp(str_datetime)
            query_data['created_at'] = {'$lte': timestamp}

        # Specify the return data and obtain it from Mongo
        return_data = {'coordinates': 1, 'place': 1, 'created_at': 1, 'entities.hashtags.text': 1, 'favorite_count': 1,
                       'entities.user_mentions.screen_name': 1, 'retweet_count': 1, 'screen_name': 1, 'text': 1,
                       'user_ud': 1}
        tweet_coordinates = list(tweets.find(query_data, return_data))

        # Check if there's data available for the dates given
        if not tweet_coordinates:
            return print("No coordinate data available for dates given.")

        # Present the user with a plot of tweet coordinates and other relevant information
        coordinates_df = pd.json_normalize(tweet_coordinates)
        coordinates_df['created_at'] = pd.to_datetime(coordinates_df['created_at'], unit='s')
        coordinates = coordinates_df['coordinates.coordinates'].values
        latitudes = [coord[1] for coord in coordinates]
        longitudes = [coord[0] for coord in coordinates]
        fig = px.scatter_geo(coordinates_df, lat=latitudes, lon=longitudes, scope='usa', color=color_on,
                             hover_data=['screen_name', 'created_at', 'place.name', 'retweet_count', 'favorite_count',
                                         'text'], color_continuous_scale='sunset',
                             title='Locations of Tweets Made by Members of US Congress')
        fig.show()
