import pandas as pd
import requests
import re
import os
import csv
import sqlite3
import json
import datetime

# Make sure you create an API key at http://www.omdbapi.com/apikey.aspx
# Assign that to the variable API_KEY
API_KEY = "14b7e235"

def create_request_url(title):
    """Create request URL from OMDPI"""
    # the request_url
    request_url = f'http://www.omdbapi.com/?t={title}&apikey={API_KEY}&type=movie&y=2019'
    return request_url

def join_tables(cur, conn):
    """Takes in the database cursor and connection as inputs. Joins the movies column from the MovieChart_2019 and the release dates from the ReleaseDateIDs tables where the release dates are equal and grab the movie title.
    Returns movie titles from MovieChart_2019"""

    cur.execute("SELECT MovieChart_2019.Movie_Title FROM MovieChart_2019 LEFT JOIN ReleaseDateIDs ON MovieChart_2019.Release_Date = ReleaseDateIDs.Date_ID")
    ids = cur.fetchall()
    conn.commit()
    return ids

def create_tables(cur, conn):
    """Takes in the database cursor and connection as inputs. Create tables for what each movie was rated along with the table for the IMDB data with creation_id, movie_title, runtime, rated, imdb_rating, and the imdb_votes.
    Returns nothing."""
    
    cur.execute("CREATE TABLE IF NOT EXISTS Rated_IDs (Rated_ID INTEGER PRIMARY KEY, Rated TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS IMDB_Data (Creation_ID INTEGER PRIMARY KEY, Movie_Title TEXT, Runtime INTEGER, Rated TEXT, IMDB_Rating TEXT, IMDB_Votes INTEGER)")
    conn.commit()


def search_in_OMDPI(cur, conn):
    """Takes in the database cursor and connection as inputs. Returns dictionary of the data retreived from OMDPI of each movie title that is in the OMDPI."""
    # calls the join_tables() functions to grab the title and assigns its return value to a variable
    movie_list = join_tables(cur, conn)
    
    # empty dictionary
    new_data = {}

    # grab each title from MovieChart_2019 table and grab the information from OMDPI
    for the_movie in movie_list:
        a_title = create_request_url(the_movie)
        data = requests.get(a_title)
        r = data.text
        json_file = json.loads(r)

         # if the response is true meaning the movie is in OMDPI, add it into a dictionary
        if json_file['Response'] == 'True':
            new_data[a_title] = json_file
    return new_data
            

def top2019_in_OMDPI(cur, conn):
    """Takes in the database cursor and connection as inputs. Grabs the titles from MovieChart_2019 and checks if the movie is in the OMDPI grabbed from search_in_OMDPI().
    Returns a list of titles that MovieChart_2019 is in OMDPI"""

    new_list  = []
    movie_list = join_tables(cur, conn)
    a_dict = search_in_OMDPI(cur, conn)

    # goes through the movies from the MovieChart_2019 list
    for i in range(len(movie_list)):
        movie = movie_list[i][0]

        for d in a_dict.values():
            #if movie from MovieChart_2019 is in the OMDPI, we want to collect data for that movie
            if movie in d.values():
                new_list.append(movie)

    # take out duplicates of the movie title
    found_title_list = []
    for i in new_list:
        if i not in found_title_list:
            found_title_list.append(i)
    return found_title_list


def grab_from_top2019_table(cur, conn):
    """Takes in the database cursor and connection as inputs. Makes sure there is a valid date in MovieChart_2019 table and not just "None". 
    Use JOIN to grab the release date from the ReleaseDateIds table that is linked to the id from MovieChart_2019 table Returns a list of tuples of the valid title and the date."""

    found_title_list = top2019_in_OMDPI(cur, conn)

    date_list = []
    for the_title in range(len(found_title_list)):
        title = found_title_list[the_title]

        # grab title and date of that title from MovieChart_2019 that was from top2019_in_OMDPI()
        cur.execute('SELECT MovieChart_2019.Movie_Title, ReleaseDateIDs.Release_Date FROM MovieChart_2019 LEFT JOIN ReleaseDateIDs ON MovieChart_2019.Release_Date = ReleaseDateIDs.Date_ID WHERE MovieChart_2019.Movie_Title = ?', (title,))
    
        the_info = cur.fetchall()
        date_list.append(the_info)   # [(title, date), (title, date), ...]
        the_title += 1

    # goes through the list of the titles and dates to make sure the date is a valid date and not "None"
    beautiful_list = []
    for get_list in (date_list):
        for get_tup in get_list:
            if get_tup[1] != 'None':
                beautiful_list.append((get_tup[0], get_tup[1]))

    # make sure there is no duplicates of the same information
    found_date_list = []
    for i in beautiful_list:
        if i not in found_date_list:
            found_date_list.append(i)
    
    return found_date_list
    # print(found_date_list)


def format_dates(cur, conn):
    """Takes in the database cursor and connection as inputs. Format the dates from the website, which can be found in the MovieChart_2019 table (Ex: Jul 19, 2019) 
    to fit how it appears in OMDPI (Ex: 19 Jul 2019). Returns a list of the formatted dates."""

    # grab the list of titles and dates from grab_from_top2019_table()
    found_date_list = grab_from_top2019_table(cur, conn)

    # emppty list for the formatted dates
    a_date_list = []

    # goes through the dates in found_date_list
    for the_date in found_date_list:
        best_date = the_date[1]

        # find the date that is formatted in MovieChart_2019 (Ex: Jul 19, 2019)
        date_time_obj = datetime.datetime.strptime(best_date, '%b %d, %Y')

        # format the date into date format in OMDPI (Ex: 19 Jul 2019)
        new_date = date_time_obj.strftime("%d %b %Y")

        # add the new formatted date to the empty list.
        a_date_list.append(new_date)

    return a_date_list


def movie_with_same_date (cur, conn):
    """Takes in the database cursor and connection as inputs. Make sure that title of the movie and the formatted date of that title is in the dictionaries from OMDPI. 
    Returns a list of the movies that has matched title and release date, which tells us that the it is the same movie from the MovieChart_2019 and OMDPI."""

    # grabs the title from the matched titles from MovieChart_2019 and OMDPI
    found_title_list = top2019_in_OMDPI(cur, conn)

    # grabs the formatted dates
    a_date_list = format_dates(cur, conn)

    # grabs the information of the movie title from OMDPI
    a_dict = search_in_OMDPI(cur, conn)

    # empty list for grabbing the title that has the matched movie title and release date
    everything_list = []

    # grab all of the titles and the dates from the found movies from OMDPI
    for i in range(len(a_date_list)):
        new_movie = found_title_list[i]
        brand_new_date = a_date_list[i]
    
        # loop through the values from the dictionary from OMDPI 
        for d in a_dict.values():

            #if movie and date from MovieChart_2019 is in the the dictionary from OMDPI, we want to collect data for that movie
            if new_movie and brand_new_date in d.values():
                everything_list.append(new_movie)

    found_every_title_list = []

    # make sure there is no duplicates of the same information
    for i in everything_list:
        if i not in found_every_title_list:
           found_every_title_list.append(i)
    return found_every_title_list
    

def rated_data(cur, conn):
    """Takes in the database cursor and connection as inputs. Returns nothing. Fills in the Rated_ID table with the Rated of the Movie Data table and their unique rated_id."""
    
    # grabs the matched title and release date 
    found_every_title_list = movie_with_same_date(cur, conn)
    
    # grab the information from dictionaries on the titles that has the Response as True from OMDPI
    a_dict = search_in_OMDPI(cur,conn)

    # empty list for the rated list
    rated_list = []
    count = 1

    # goes through the title in the matched title and release date
    for wanted_title in found_every_title_list:

        # goes through the information from the OMDPI
        for y in a_dict:

                # makes sure that the title from OMDPI and the title from the "matched title and release date" are the same
                if a_dict[y]['Title'] == wanted_title:

                        # grabs the rate of the true match title from both OMDPI and the "matched title and release date"
                        rated = a_dict[y]['Rated']

                        # makes sure that the rate is not already put into the table
                        if rated not in rated_list:
                            rated_id = count
                            rated_list.append(rated)
                            cur.execute("INSERT OR IGNORE INTO Rated_IDs (Rated_ID, Rated) VALUES (?, ?)", (rated_id, rated))
                            count += 1  
    conn.commit()

def put_data(cur, conn):
    """"Takes in the database cursor and connection as inputs. Returns nothing. Fills in the IMDB_Data table with the Movie_Title and its unique ID, the runtime of the movie, what the movie is rated as,
    the IMDB_Rating of the movie, and the IMDB_Votes of the movie."""

    # grabs the matched title and release date 
    found_every_title_list = movie_with_same_date(cur, conn)

    # grab the information from dictionaries on the titles that has the Response as True from OMDPI
    a_dict = search_in_OMDPI(cur, conn)

    # empty list for the movie title list
    another_title_list = []

    # find the length of the table, meaning the amount of rows
    cur.execute("SELECT * FROM IMDB_Data")
    x = len(cur.fetchall())

    # goes through 25 times every time the play button is played to retrieve new information 25 times.
    for i in range(0,25):

        # goes through the dictionary from search_in_OMDPI()
        for y in a_dict:

            # check if the length of the movie list is less than or equal to what we currently have, if so, break
            if len(found_every_title_list) <= i + x:
                break

            # otherwise, check if the title is in the another_title_list list and if not, continue
            if found_every_title_list[i + x] not in another_title_list:  
        
                # match the title from the OMDPI with the title from the "matched title and release date" that was found for OMDPI and MovieChart_2019
                if a_dict[y]['Title'] == found_every_title_list[i + x]:

                    # grab the rating of the movie and if the rating is "N/A", make the rating a 0
                    rating = a_dict[y]['imdbRating']
                    if rating == "N/A":
                        rating = rating.replace("N/A", "0")

                    # grab the rated of the movie and match it to the id to put the rated_id from Rated_IDs table to the IMDB_Data
                    rated = a_dict[y]['Rated']
                    cur.execute('SELECT Rated_ID FROM Rated_IDs WHERE Rated = ?', (rated,))
                    rated_id = cur.fetchone()[0]

                    # grab the runtime of the movie and if the runtime is "N/A", make the runtime a 0
                    some_runtime = a_dict[y]['Runtime'].replace("min", "")
                    if some_runtime == "N/A":
                        some_runtime = some_runtime.replace("N/A", "0")

                    # grab the IMDB votes of the movie and if the IMDB votes is "N/A", make the IMDB votes a 0
                    votes = a_dict[y]['imdbVotes'].replace(",","")
                    if votes == "N/A":
                        votes = votes.replace("N/A", "0")
                    
                    # id count for the movie title and its information in IMDB_Data
                    key = i + x + 1
                    cur.execute("INSERT OR IGNORE INTO IMDB_Data (Creation_ID, Movie_Title, Runtime, Rated, IMDB_Rating, IMDB_Votes) VALUES (?, ?, ?, ?, ?, ?)", (key, found_every_title_list[i + x], some_runtime, rated_id, rating, votes))
     
        # if the length of the list is greater than the indexing, append the rest 
        if len(found_every_title_list) > i + x:
            another_title_list.append(found_every_title_list[i + x])
   
    conn.commit()

def average_gross_runtime(cur, conn):
    """Takes in the database cursor and connection as inputs. Grabs a list of tuples with the movie title, the runtime from IMDB_Data and the movie's gross from MovieChart_2019
    where the movies are the same using a left join. Returns the a tuple of strings of the top five movies with the highest gross per 1 minute of runtime based on the IMDB_Data."""

    # using JOIN to make sure the Movie_Title from the IMDB_Data and MovieChart_2019 are the same 
    # as well as collected the selected information we want to grab (Movie_Title from MovieChart_2019, Runtime from IMDB_Data, and Gross_2019 form MovieChart_2019)
    cur.execute("SELECT IMDB_Data.Movie_Title, IMDB_Data.Runtime, MovieChart_2019.Gross_2019 FROM IMDB_Data LEFT JOIN MovieChart_2019 ON IMDB_Data.Movie_Title = MovieChart_2019.Movie_Title")
  
    results  = cur.fetchall()

    top_five = []
    price_list = []

    # go through the tuple and assign the gross and the runtime to their associating variables and make them into a float so it can grab the decimals
    for data in results:
        the_gross = float(data[2])
        the_runtime = float(data[1])

        # if the runtime is not 0, the calculation of gross / runtime is calculated for that movie
        try:
            price = float(the_gross / the_runtime)
        
        # if the runtime is 0, there will be a ZeroDivisionError, and if that occurs, the calculation will be 0 for that movie.
        except ZeroDivisionError:
            price = 0
        
        price_list.append((data[0], price))

    # highest calculation to the lowest
    descending_price = sorted(price_list, key = lambda x: x[1], reverse= True)

    #appends the first five price for ticket in the descending_price list to the top_five list.
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_price[0][0] + "\" gross per 1 minute runtime is $" + str(descending_price[0][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_price[1][0] + "\" gross per 1 minute runtime is $" + str(descending_price[1][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_price[2][0] + "\" gross per 1 minute runtime is $" + str(descending_price[2][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_price[3][0] + "\" gross per 1 minute runtime is $" + str(descending_price[3][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_price[4][0] + "\" gross per 1 minute runtime is $" + str(descending_price[4][1]))
    return top_five


def get_runtime(cur, conn):
    """Takes the database cursor and connection as inputs. Returns a string with the average runtime of all movies in the chart."""
    # empty list to store the runtime of all the movies.
    runtime_2019 = []

    # grab all the runtime from the movies and put the numbers in a list
    cur.execute('SELECT Runtime FROM IMDB_Data')
    entire_runtime = cur.fetchall()

    # go through all the runtimes and append only the runtime number since we grabbed a tuple from before
    for runtime in entire_runtime:
        runtime_2019.append(int(runtime[0]))

    # add the runtime for each movie into the variable, the_sum
    the_sum = 0
    for x in runtime_2019:
        the_sum = the_sum + x

    # finds the average runtime in the Top Grossing Movies of 2019 of all the movie
    # by dividing the sum of the numbers in the list by the length of the list and get only two decimals.
    average_runtime = float(str(round(the_sum / len(runtime_2019),  2)))
    return("The average runtime of all the movies from the Top Grossing Movies of 2019 is " + str(average_runtime)) + " mins."


def write_data_to_file(filename, cur, conn):

    """Takes in a filename (string), the database cursor and connection as inputs. Returns nothing. 
    Creates a file and writes return values of the functions find_top_five_movie_price() and get_average_gross() to the file. """
    
    # once the table is done being filled (once it reaches 300 rows), the calculations are written to a file.
    cur.execute('SELECT Movie_Title FROM MovieChart_2019')
    titles = cur.fetchall()
    if len(titles) == 300:

        path = os.path.dirname(os.path.abspath(__file__)) + os.sep

        outFile = open(path + filename, "w")

        outFile.write("Average Runtime of the Top 300 Grossing Movies Reported in the Top Grossing Movies of 2019\n")
        outFile.write("=====================================================\n\n")
        weeks_output = str(get_runtime(cur, conn))
        outFile.write(weeks_output + "\n\n")
        outFile.write("Top Five Gross Per Minute From the Top 300 Grossing Movies Reported in the Top Grossing Movies of 2019\n")
        outFile.write("======================================================\n\n")
        top_five_prices = average_gross_runtime(cur, conn)
        outFile.write("1. " + top_five_prices[0] + "\n")
        outFile.write("2. " + top_five_prices[1] + "\n")
        outFile.write("3. " + top_five_prices[2] + "\n")
        outFile.write("4. " + top_five_prices[3] + "\n")
        outFile.write("5. " + top_five_prices[4] + "\n")
        outFile.close()




def main():
    """Takes nothing as an input and returns nothing. 
    Calls the functions join_tables(), set_up_tables(), create_tables(), ssearch_in_OMDPI(), top2019_in_OMDPI(), grab_from_top2019_table(), format_dates(),
    movie_with_same_date(), rated_data(), put_data(), and write_data_to_file(). Closes the database connection."""

    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/movies.db')
    cur = conn.cursor()
    join_tables(cur, conn)
    create_tables(cur, conn)
    search_in_OMDPI(cur, conn)
    top2019_in_OMDPI(cur, conn)
    grab_from_top2019_table( cur, conn)
    format_dates(cur, conn)
    movie_with_same_date (cur, conn)
    rated_data(cur, conn)
    put_data(cur, conn)
    write_data_to_file("omdb_data.txt", cur, conn)
    conn.close()

if __name__ == "__main__":
    main()

