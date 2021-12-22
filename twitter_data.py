import requests
import json
import tweepy 
import sqlite3          
import os

consumer_key = "NJGG4V4puH39ZyMiOojs4BBJ5"
consumer_secret = "rS78OXKZucVBlWtOzBIK1M0FoHTkev68LXdM859UK4QYs8xPRb"
access_token = "1383133286629847043-v90SS5wUs90A4SpAcIZDhUzypESi5G"
access_token_secret = "3axQql0Kgmr4hb8pJeAtZsWOK93uShUYW84Jor1fDjduS"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, timeout=1200, parser=tweepy.parsers.JSONParser(), wait_on_rate_limit=True, retry_count=3)



def create_twitter_table(cur, conn):
    """Takes in the database cursor and connection as inputs. Returns nothing. Creates Twitter_Data table. 
    Uses the Movie_Title column from the MovieChart_2019 table to find tweets containing the movie title. 
    Adds number of favorites per search, followers, number of mentions, retweets, friends count, listed count, and statuses count per search for each of the movies."""
   
    cur.execute("CREATE TABLE IF NOT EXISTS Twitter_Data (Twitter_Id INTEGER UNIQUE, Movie_Title TEXT UNIQUE, Movie_Mentions INTEGER, Movie_Favorited INTEGER, Follower_Count INTEGER, Retweet INTEGER, Friends_Count INTEGER, Listed_Count INTEGER, Statuses_Count INTEGER, PRIMARY KEY(Twitter_Id AUTOINCREMENT))")
    
    # grab movie titles from the MovieChart_2019 table
    cur.execute('SELECT Movie_Title FROM MovieChart_2019')
    movie_list = cur.fetchall()

    # grab movie titles from the Twitter_Data
    cur.execute('SELECT Movie_Title FROM Twitter_Data')
    movie_names = cur.fetchall()

    # empty list for the movies 
    existing_movies = []

    # add name from Twitter_Data into the list and find the length of the list
    for name in movie_names:
        existing_movies.append(name[0])
    key = len(movie_names)

    # goes through the mvoies from MovieChart_2019
    for movies in movie_list:
        movie = movies[0]

        # search for the movie in Twitter and grab the results from the search
        results = api.search(q=str(movie), count=1000000)
        retweet_count = 0
        mentions_count = 0
        favorite_count = 0
        follower_count = 0
        friends_count  = 0
        listed_count = 0
        statuses_count = 0

        # for all of the results, grab the favorite_count and retweet_count of the movie. 
        # also grab the followers_count, friends_count, listed_count, and statuses_count of the user that mentioned the movie
        for result in results['statuses']:
            fav_count = result['favorite_count']
            favorite_count += fav_count
            tweet_count = result['retweet_count']
            retweet_count += tweet_count
            fol_count = result['user']['followers_count']
            follower_count += fol_count
            friend_count = result['user']['friends_count']
            friends_count += friend_count
            list_count =  result['user']['listed_count']
            listed_count += list_count
            status_count = result['user']['statuses_count']
            statuses_count += status_count
            mentions_count += 1
        key += 1

        # put the data into the Twitter_Data table
        cur.execute("INSERT OR IGNORE INTO Twitter_Data (Twitter_Id, Movie_Title, Movie_Mentions, Movie_Favorited, Follower_Count, Retweet, Friends_Count, Listed_Count, Statuses_Count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
        (key, movie, mentions_count, favorite_count, follower_count, retweet_count, friends_count, listed_count, statuses_count))
 
    conn.commit()

    
def the_table(cur, conn):
    """Takes the database cursor and connection as inputs. Returns a list of tuples with the movie title, the number of mentions for that movie, and the follower count of that movie from Twitter_Data."""
    cur.execute("SELECT Twitter_Data.Movie_Title, Twitter_Data.Movie_Mentions, Twitter_Data.Follower_Count FROM Twitter_Data")
    results = cur.fetchall()
    conn.commit()
    return results


def average_followers_per_movie(cur, conn):
    """Takes the database cursor and connection as inputs. Returns a list of strings specifying the average amount of followers of a Twitter user tweeting about a certain movie. If the movie had no mentions, that is specified. """
    # empty list to store the average followers of each movie.
    average_list = []
    results = the_table(cur, conn)
    for result in results:

        # if the result has more than 0 mentions, the average is calculated (movie's follower count / movie's mentions) and appended to the list.
        if result[1] > 0:
            average = result[2] / result[1]
            average_list.append("The average number of followers of a Twitter user tweeting about " + result[0] + " is " + str(average))

        # if the movie does not have any mentions on Twitter, it is specified in the list.
        else:
            average_list.append("The movie " + result[0] + " did not have any mentions on Twitter.")
    return average_list


def favorites_vs_gross(cur, conn):
    """Takes the database cursor and connection as inputs. Grabs a list of tuples with the movie title, the gross from MovieChart_2019 and the movie's favorited count from Twitter_Data
    where the movies are the same using a left join. Returns the a tuple of strings of the top five movies with the highest gross per 1 favorite on Twitter."""

    # using JOIN to make sure the Movie_Title from the Twitter_Data and MovieChart_2019 are the same 
    # as well as collected the selected information we want to grab (Movie_Title from MovieChart_2019, Gross from MovieChart_2019 and Movie_Favorited)
    cur.execute("SELECT MovieChart_2019.Movie_Title, MovieChart_2019.Gross_2019, Twitter_Data.Movie_Favorited FROM MovieChart_2019 LEFT JOIN Twitter_Data ON MovieChart_2019.Movie_Title = Twitter_Data.Movie_Title")
    results  = cur.fetchall()

    top_five = []
    calculation_list = []

    for data in results:
        the_gross = float(data[1])
        the_movie_favorited= data[2]
        movie_favorited = float(the_movie_favorited)
        
        # if the movie_favorited is not 0, the calculation of gross / movie_favorited is calculated for that movie
        try:
            calculation = float(the_gross / movie_favorited)

        # if the movie_favorited is 0, there will be a ZeroDivisionError, and if that occurs, the calculation will be 0 for that movie.
        except ZeroDivisionError:
            calculation = 0
        
        calculation_list.append((data[0], calculation))
    
    # highest calculation to the lowest
    descending_versus = sorted(calculation_list, key = lambda x: x[1], reverse= True)

    #appends the first five gross per 1 favorite on Twitter from the descending_versus list to the top_five list.
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_versus[0][0] + "\" gross per 1 favorite on Twitter is $" + str(descending_versus[0][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_versus[1][0] + "\" gross per 1 favorite on Twitter is $" + str(descending_versus[1][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_versus[2][0] + "\" gross per 1 favorite on Twitter is $" + str(descending_versus[2][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_versus[3][0] + "\" gross per 1 favorite on Twitter is $" + str(descending_versus[3][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_versus[4][0] + "\" gross per 1 favorite on Twitter is $" + str(descending_versus[4][1]))
    return top_five
   

def write_data_to_file(filename, cur, conn):
    """Takes the name of a file (string), database cursor and connection as inputs. Returns nothing. Writes the result of the function average_followers_per_movie() and favorite_vs_gross() to a file. """

    path = os.path.dirname(os.path.abspath(__file__)) + os.sep

    # writes the results of the average_followers_per_movie() function to a file. Outputs all of the movies average followers per mentions
    outFile = open(path + filename, "w")
    outFile.write("Average Followers of a Twitter User Based on the Movie They Tweet About\n")
    outFile.write("=======================================================================\n\n")
    average_output = average_followers_per_movie(cur, conn)
    for data in average_output:
        outFile.write(str(data) + '\n' + '\n')

    # writes the results of the favorite_vs_gross() function to a file.
    outFile.write("Top Five Gross Per Favorite From Twitter on the Top 300 Grossing Movies Reported in the Top Grossing Movies of 2019\n")
    outFile.write("======================================================\n\n")
    top_five_gross = favorites_vs_gross(cur, conn)
    outFile.write("1. " + top_five_gross[0] + "\n")
    outFile.write("2. " + top_five_gross[1] + "\n")
    outFile.write("3. " + top_five_gross[2] + "\n")
    outFile.write("4. " + top_five_gross[3] + "\n")
    outFile.write("5. " + top_five_gross[4] + "\n")

    outFile.close()

def main():
    """Takes nothing as an input and returns nothing. Calls the functions create_twitter_table() and write_data_to_file(). Closes the database connection. """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/movies.db')
    cur = conn.cursor()
    create_twitter_table(cur, conn)
    write_data_to_file("twitter_data.txt", cur, conn)

    conn.close()

if __name__ == "__main__":
    main()

