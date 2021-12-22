from bs4 import BeautifulSoup
import requests
import re
import os
import csv
import sqlite3
import json

def get_2019_movies():
    """Returns a list of tuples in the format (movie_title, release_date, distributor, genre, gross_2019, and tickets_sold)
    Uses BeautifulSoup to read the movie, release_date, distributor, genre, grossing, and ticket sold on the chart and only grab 300"""
    # empty lists to collect the names of movie, release_date, distributor, genre, grossing, and ticket sold on the list.
    movie_list = []
    date_list = []
    distributor_list = []
    genre_list = []
    gross_list  = []
    ticket_list = []

    # using beautiful soup to get data from the Top Grossing Movies of 2019
    base_url = 'https://www.the-numbers.com/market/2019/top-grossing-movies'
    r = requests.get(base_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    table = soup.find("table")
    tr = table.find_all("tr")

    for i in tr[1:301]:
        a_href = i.find_all("a")
        movies = a_href[0].text
        movie_list.append(movies)

    # if there is an empty space for date in the chart, replace it with "None" instead
    for i in tr[1:301]:
        a_href = i.find_all("a")
        if len(a_href) < 3:
            date_list.append("None")
        else:
            date = a_href[1].text
            date_list.append(date)

    # if there is an empty space for distributor in the chart, replace it with "None" instead
    for i in tr[1:301]:
        a_href = i.find_all("a")
        if len(a_href) < 3:
            distributor_list.append("None")
        else:
            distributor = a_href[2].text
            distributor_list.append(distributor)

    # if there is an empty space for genre in the chart, replace it with "None" instead
    for i in tr[1:301]:
        a_href = i.find_all("a")
        if len(a_href) < 4:
            genre_list.append("None")
        else:
            genres = a_href[3].text
            genre_list.append(genres)

    # take out the "$" and the "," to make the the number an integer for calculations
    for i in tr[1:301]:
        td = i.find_all("td")
        a_gross = td[5].text.replace(',', '')
        grossing = int(a_gross.replace('$', ''))
        gross_list.append(grossing)

    # take out the "," to make the number an integer for calculations
    for i in tr[1:301]:
        td = i.find_all("td")
        tickets = int(td[6].text.replace(',', ''))
        ticket_list.append(tickets)

    x = 0
    # empty to list collect tuples in the format (movie, release_date, distributor, genre, 2019_gross, tickets_sold)
    movie_info_list = []
    
    for x in range(len(movie_list)):
       
        movie_info_list.append((movie_list[x], date_list[x], distributor_list[x], genre_list[x], gross_list[x], ticket_list[x]))

        x = x + 1
    return movie_info_list
 
def setUpDatabase(db_name):
    """Takes the name of a database, a string, as an input. Returns the cursor and connection to the database."""
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + '/'+ db_name)
    cur = conn.cursor()
    return cur, conn

def created_tables(cur, conn):
    """Create ReleaseDateIds, DistributorIds, GenreIDs, and MovieChart_2019 table"""

    cur.execute("CREATE TABLE IF NOT EXISTS ReleaseDateIDs (Date_Id INTEGER UNIQUE, Release_Date TEXT UNIQUE, PRIMARY KEY(Date_Id AUTOINCREMENT))")
    cur.execute("CREATE TABLE IF NOT EXISTS DistributorIDs (Distributor_Id INTEGER UNIQUE, Distributor TEXT UNIQUE, PRIMARY KEY(Distributor_Id AUTOINCREMENT))")
    cur.execute("CREATE TABLE IF NOT EXISTS GenreIDs (Genre_Id INTEGER PRIMARY KEY, Genre TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS MovieChart_2019(Creation_Id INTEGER PRIMARY KEY, Movie_Title TEXT, Release_Date TEXT, Distributor TEXT, Genre TEXT, Gross_2019 INTEGER, Tickets_Sold INTEGER)")
    conn.commit()


def set_up_date_id_table(cur, conn):
    """Takes the database cursor and connection as inputs. Returns nothing. 
    Fills in the ReleaseDateIds table with the Release_Date in the MovieChart_2019 table and their unique date_id."""
    
    #Calls the get_2019_movies() function and saves it into a list of dict
    the_2019_list = get_2019_movies()

    count = 0
    i = 0
    # grab length of the the ReleaseDateIDs table
    cur.execute("SELECT * FROM ReleaseDateIDs")
    date_id = len(cur.fetchall())

    # while the count does not reach 25, check if the length of the movie list is less than or equal to what we currently have, if so, break
    while True:
        if len(the_2019_list) <= date_id + i:
            break

        # otherwise, add the id
        date = the_2019_list[date_id + i][1]

        i = i + 1
        try:
            num = date_id + count + 1
            cur.execute("INSERT INTO ReleaseDateIDs (Date_Id, Release_Date) VALUES (?, ?)", (num, date,))
            count = count + 1

            # if count reaches 25, break
            if count >=  25:
                break
        except:
            continue

    conn.commit()

def set_up_distributor_id_table(cur, conn):
    """Takes the database cursor and connection as inputs. Returns nothing. 
    Fills in the DistributorIDs table with the distributor in the Movie Data table and their unique distributor_id."""
    
    #Calls the get_2019_movies() function and saves it into a list of dict
    the_2019_list = get_2019_movies()
    count = 0
    i = 0

    # grab length of the the DistributorIDs table
    cur.execute("SELECT * FROM DistributorIDs")
    dis_id = len(cur.fetchall())

    # while the count does not reach 25, check if the length of the movie list is less than or equal to what we currently have, if so, break
    while True:
        if len(the_2019_list) <= dis_id + i:
            break

        # otherwise, add the id
        distributor = the_2019_list[dis_id + i][2]
        i = i + 1
        try:
            num =  dis_id + count + 1
            cur.execute("INSERT INTO DistributorIDs (Distributor_Id, Distributor) VALUES (?, ?)", (num, distributor,))
            count = count + 1

            # if count reaches 25, break
            if count >= 25:
                break
        except:
            continue
       
    conn.commit()


def set_up_genre_id_table(cur, conn):
    """Takes the database cursor and connection as inputs. Returns nothing. 
    Fills in the GenreIds table with the genres in the Movie Data table and their unique genre_id."""
    
    #Calls the get_2019_movies() function and saves it into a list of dict
    the_2019_list = get_2019_movies()
    
    genre_list = []

    # loops through the the_2019_list and adds in the genre and their id number
    count = 1

    # since there's only 14 genres, it it okay to take all at once
    for x in range(len(the_2019_list)):
        
        # only adds in the genre to the genre_list if it is not already in to genre_list
        if the_2019_list[x][3] not in genre_list:
            genre_id = count
            genre = the_2019_list[x][3]
            genre_list.append(genre)
            cur.execute("INSERT OR IGNORE INTO GenreIDs (Genre_Id, Genre) VALUES (?, ?)", (genre_id, genre))
            
            count = count + 1
        x = x + 1
    conn.commit()


def fillup_table(cur, conn):
    """Takes the database cursor and connection as inputs. Does not return anything. 
    Fills in the MovieChart_2019 table. The creation_id is each movie's unique identification number."""

    #calls get_2019_movies() to get the movies from the Top Grossing Movies of 2019
    the_2019_movies = get_2019_movies()

    count = 0
    i = 0

    # grab length of the the MovieChart_2019table
    cur.execute('SELECT * FROM MovieChart_2019')
    the_id = len(cur.fetchall())

    # while the count does not reach 25, check if the length of the movie list is less than or equal to what we currently have, if so, break
    while True:
        if len(the_2019_movies) <= the_id + i:
            break

        # otherwise, continue and fill up the data table
        movie = the_2019_movies[the_id + i][0]
        date = the_2019_movies[the_id + i][1]
        distributor = the_2019_movies[the_id + i][2]
        genre = the_2019_movies[the_id + i][3]
        gross = the_2019_movies[the_id + i][4]
        ticket = the_2019_movies[the_id + i][5]
        i = i + 1
    
        try:
            num =  the_id + count + 1
            #In order to save storage space, we are adding in GenreIds (integers) instead of genre.
            cur.execute('SELECT genre_id FROM GenreIDs WHERE genre = ?', (genre,))
            genre_id = cur.fetchone()[0]

            #In order to save storage space, we are adding in DistributorIDs (integers) instead of distributor.
            cur.execute('SELECT Distributor_id FROM DistributorIDs WHERE Distributor = ?', (distributor,))
            distributor_id = cur.fetchone()[0]

            #In order to save storage space, we are adding in Date_ID (integers) instead of the release date.
            cur.execute('SELECT Date_Id FROM ReleaseDateIDs WHERE Release_Date = ?', (date,))
            date_id = cur.fetchone()[0]
            
            cur.execute("INSERT OR IGNORE INTO MovieChart_2019 (Creation_Id, Movie_Title, Release_Date, Distributor, Genre, Gross_2019, Tickets_Sold) VALUES (?, ?, ?, ?, ?, ?, ?)", (int(num), movie,int(date_id), int(distributor_id), int(genre_id), gross, ticket))
            count = count + 1

            # if count reaches 25, break
            if count >= 25:
                break
        except:
            continue
       
    conn.commit()

def get_average_gross(cur, conn):
    """Takes the database cursor and connection as inputs. Returns a string with the average gross of all movies in the chart."""

    # empty list to store the grossing of all the movies.
    gross_2019 = []

    # grab all the grossing of the movies from MovieChart_2019 table land put the numbers in a list
    cur.execute('SELECT gross_2019 FROM MovieChart_2019')
    grosses = cur.fetchall()

    # go through all the grossing and append only the grossing number since we grabbed a tuple from before
    for gross in grosses:
        gross_2019.append(int(gross[0]))

    # add the grossing for each movie into the variable, the_sum
    the_sum = 0
    for x in gross_2019:
        the_sum = the_sum + x
    
    #finds the average grossings in the Top Grossing Movies of 2019 of all the movie
    #by dividing the sum of the numbers in the list by the length of the list and get only two decimals.
    average_gross = float(str(round(the_sum / len(gross_2019),  2)))
    return("The average gross of all the movies from the Top Grossing Movies of 2019 is $" + str(average_gross))

    
def find_top_five_movie_price(cur, conn):
    """Takes in the database cursor and connection as inputs. Returns a list of strings (the five movies with priciest ticket on the Top Grossing Movies of 2019 chart)."""

    # empty lists to store the movie name, the gross, and the ticket sold for each movie 
    data_list = []
    price_list  = []
    top_five  = []

    # get the movie title, grossing, and ticket sold for each movie from MovieChart_2019 table
    cur.execute('SELECT Movie_Title, Gross_2019, Tickets_Sold FROM MovieChart_2019')
    the_data = cur.fetchall()

    for a_data in the_data:
        data_list.append(a_data)
    
    # get the gross divided by tickets sold to get the price of tickets for each movie
    for data in data_list:
        the_gross = float(data[1])
        sold_tickets = float(data[2])
        ticket_price = float(the_gross / sold_tickets)
        price_list.append((data[0], ticket_price)) 

    #sort items descending 
    descending_price = sorted(price_list, key = lambda x: x[1], reverse= True)

    # appends the first five price for ticket in the descending_price list to the top_five list.
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_price[0][0] + "\" price per ticket is $" + str(descending_price[0][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_price[1][0] + "\" price per ticket is $" + str(descending_price[1][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_price[2][0] + "\" price per ticket is $" + str(descending_price[2][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_price[3][0] + "\" price per ticket is $" + str(descending_price[3][1]))
    top_five.append("From the Top Grossing Movies of 2019, \"" + descending_price[4][0] + "\" price per ticket is $" + str(descending_price[4][1]))
    return top_five
   

def write_data_to_file(filename, cur, conn):
    """Takes in a filename (string), the database cursor and connection as inputs. Returns nothing. 
    Creates a file and writes return values of the functions find_top_five_movie_price() and get_average_gross() to the file. """
    # once the table is done being filled (once it reaches 300 rows), the calculations are written to a file.

    cur.execute('SELECT Movie_Title FROM MovieChart_2019')
    titles = cur.fetchall()
    if len(titles) == 300:

        path = os.path.dirname(os.path.abspath(__file__)) + os.sep

        outFile = open(path + filename, "w")

        outFile.write("Average Grossing of All the Movie Reported in the Top Grossing Movies of 2019\n")
        outFile.write("=====================================================\n\n")
        weeks_output = str(get_average_gross(cur, conn))
        outFile.write(weeks_output + "\n\n")
        outFile.write("Top Five Ticket Price from the Top Grossing Movies of 2019\n")
        outFile.write("======================================================\n\n")
        top_five_prices = find_top_five_movie_price(cur, conn)
        outFile.write("1. " + top_five_prices[0] + "\n")
        outFile.write("2. " + top_five_prices[1] + "\n")
        outFile.write("3. " + top_five_prices[2] + "\n")
        outFile.write("4. " + top_five_prices[3] + "\n")
        outFile.write("5. " + top_five_prices[4] + "\n")
        outFile.close()


def main():
    """Takes nothing as an input and returns nothing. 
    Calls the functions created_tables(), set_up_genre_id_table(), set_up_date_id_table(), 
    set_up_distributor_id_table(), fillup_table(), and write_data_to_file(). Closes the database connection."""
    cur, conn = setUpDatabase('movies.db')
    created_tables(cur, conn)
    set_up_genre_id_table(cur, conn)
    set_up_date_id_table(cur, conn)
    set_up_distributor_id_table(cur,conn)
    fillup_table(cur, conn)
    write_data_to_file("movie_data.txt", cur, conn)
    conn.close()

if __name__ == "__main__":
    main()