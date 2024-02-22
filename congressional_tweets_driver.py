"""
DS4300
Mia Huebscher
MongoDB Tutorial with Congressional Tweet Data, Spring 2024

Utilizes an API to query for data regarding tweets made by US Congress members and analyze it.
"""
import string
from pySankey import sankey
import matplotlib.pyplot as plt
from collections import Counter
from wordcloud import WordCloud
from nltk.corpus import stopwords
from congressional_tweets_api import congressionalTweets


def most_common_words(text, n=10):
    """
    Identifies the n most common words in a text
    :param text: the text to analyze
    :param n: the number of most common words to retrieve
    :return: a dictionary of the most common words
    """
    # Remove stopwords and punctuation from the text
    text = text.translate(str.maketrans('', '', string.punctuation.replace('@', '')))
    txt_lst = [wd.strip() for wd in text.lower().split(' ')]
    stopwds_dct = Counter(stopwords.words('english') + ['like', 'im', 'rt'])
    text = [wd for wd in txt_lst if wd not in stopwds_dct and ('@' or ' ') not in wd and wd[:4] != 'http' and
            wd.isdigit() is False and wd != '']

    # Identify the n most common words
    common_words = Counter(text).most_common(n)

    return common_words


if __name__ == "__main__":
    # Create the API instance
    congressionalTweets = congressionalTweets()

    """
    Question 1: What are the 15 most common words in the 100 most popular tweets made by Congress members?
    """
    # Aggregate the text from the 100 most popular tweets
    all_text = ""
    popular_tweets = congressionalTweets.most_popular_tweets(n=100, popularity_metric="retweet_count")
    for tweet in popular_tweets:
        text = tweet['text']
        all_text += text + ' '

    # Identify the 15 most common words and
    common_words = most_common_words(all_text.strip(), n=15)
    print("The 15 most common words in the 100 most popular tweets made by U.S. Congress Members:")
    for item in common_words:
        print(item[0] + f' (Count: {item[1]})')

    # Visualize the 15 most common words in a wordcloud
    wordcloud = WordCloud(width=1000, height=1000, background_color='white',
                          colormap='jet').generate(''.join([item[0] + ' ' for item in common_words]))
    plt.title(f"The 15 Most Common Words in the 100 Most Popular \n Tweets Made by Congress Members")
    plt.imshow(wordcloud)
    plt.axis('off')
    plt.show()

    """
    Question 2: What are the 15 most common hashtags in the 100 most popular tweets made by Congress members?
    """
    # Aggregate the hashtags from the 100 most popular tweets containing hashtags
    hashtag_lst = []
    popular_tweets = congressionalTweets.most_popular_tweets(n=100, popularity_metric="retweet_count",
                                                             require_hashtags=True)
    for tweet in popular_tweets:
        hashtags = tweet['entities']['hashtags']
        for i in range(len(hashtags)):
            hash = hashtags[i]['text']
            hashtag_lst.append(hash)

    # Identify the 15 most common hashtags
    common_hashs = Counter(hashtag_lst).most_common(15)
    print("\nThe 15 most common hashtags in the 100 most popular tweets made by U.S. Congress Members:")
    for item in common_hashs:
        print(item[0] + f' (Count: {item[1]})')

    # Visualize the 15 most common hashtags in a wordcloud
    wordcloud = WordCloud(width=1000, height=1000, background_color='white',
                          colormap='jet').generate(''.join([item[0] + ' ' for item in common_hashs]))
    plt.title(f"The 15 Most Common Hashtags in the 100 Most Popular \n Tweets Made by Congress Members")
    plt.imshow(wordcloud)
    plt.axis('off')
    plt.show()

    """
    Question 3: How do the common words in twitter posts vary among the users, @POTUS, @SenSanders, and @SenTedCruz
    """
    # Initialize variables
    users = ['POTUS', 'SenSanders', 'SenTedCruz']
    words = []
    counts = []

    # For each user, aggregate the text from the user's 50 most recent tweets
    for user in users:
        user_tweets = congressionalTweets.get_user_tweets(username=user, n=50)
        full_user_txt = ""
        for tweet in user_tweets:
            text = tweet['text']
            full_user_txt += text

        # Identify the 10 most common words in the user's tweets and append the data to lists
        wd_freqs = most_common_words(full_user_txt, n=10)
        common_words = [wd[0] for wd in wd_freqs]
        word_counts = [wd[1] for wd in wd_freqs]
        words.append(common_words)
        counts.append(word_counts)

    # Print each user's 10 most common words
    for i in range(len(users)):
        print(f'\n10 Most Common Words Used in Posts Made by {users[i]}:')
        for word in words[i]:
            print(word)

    # Utilize the collected data to create a sankey diagram
    source_nodes = [users[0]]*10 + [users[1]]*10 + [users[2]]*10
    target_nodes = words[0] + words[1] + words[2]
    thickness = counts[0] + counts[1] + counts[2]
    sankey.sankey(source_nodes, target_nodes, thickness, figure_name='common_words', aspect=10, fontsize=10)
    plt.title("Comparison of the 10 Most Common Words in the 50 Most Recent \n Tweets by " + 
              f"Users: {users[0]}, {users[1]}, and {users[2]}")
    plt.show()

    """
    Questions 4: Which years in the data were the most active for Tweets made by Congress members
    """
    # Obtain the number of tweets made each year in the data
    year_counts = congressionalTweets.tweets_by_time(agg_by='year')

    # Identify the most popular year for tweets
    years = [item['_id'] for item in year_counts]
    counts = [item['Count'] for item in year_counts]
    max_count_idx = counts.index(max(counts))
    print(f"\nThe most active year for tweets made by Congress members was {years[max_count_idx]}, with {max(counts)}",
          "tweets being posted that year!")

    # Create a line chart for the data
    plt.title('Number of Tweets Made by U.S. Congress Members per Year from 2008-2017')
    plt.plot(years, counts, linestyle='-', marker='o', color='darkcyan')
    plt.xlabel('Years')
    plt.ylabel('Number of Tweets')
    plt.xticks(years)
    plt.show()

    """
    Question 5: Where in the United States were there high and low concentrations of tweets made by Congress members in 
    2016?
    """
    # Call the api that generates a plot of tweet locations with creation dates after the beginning of 2011
    congressionalTweets.geography(color_on='retweet_count', earliest_date='01/01/2016', latest_date='12/31/2016')

    # Close the database connection
    congressionalTweets.close_connection()








