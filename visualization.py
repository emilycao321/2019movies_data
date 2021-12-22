import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from bs4 import BeautifulSoup
import requests
import re
import os
import csv
import sqlite3
import json


def main():
    """Takes no inputs and returns nothing. Selects data from the database in order to create visualizations (two dot plots, a scatterplot, and two bar charts.) """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/movies.db')
    cur = conn.cursor()

    
    #creates a list of tuples with the movies and the follower counts from the Follower_Data table 
    cur.execute('SELECT Follower_Count, Movie_Title FROM Twitter_Data')
    follower_counts_list = cur.fetchall()
    follower_counts = []
    movie_names = []
    for tup in follower_counts_list:
        follower_counts.append(tup[0])
        movie_names.append(tup[1])

    # creates a list of the Twitter favorites each movie has per about 100 mentions.
    cur.execute('SELECT Movie_Favorited FROM Twitter_Data')
    favorite_counts_list = cur.fetchall()
    favorite_counts = []
    for tup in favorite_counts_list:
        favorite_counts.append(tup[0])

    # creates a list of the rankings of each movie. the unique id of each movie is the same as its ranking on the top movies 2019 chart.
    cur.execute('SELECT Creation_ID FROM MovieChart_2019')
    rank_list = cur.fetchall()
    rank_on_chart = []
    for tup in rank_list:
        rank_on_chart.append(tup[0])


    # creates a three lists by using a LEFT JOIN.
    cur.execute("SELECT IMDB_Data.Runtime, IMDB_Data.Movie_Title, MovieChart_2019.Gross_2019 FROM IMDB_Data LEFT JOIN MovieChart_2019 ON MovieChart_2019.Movie_Title = IMDB_Data.Movie_Title")
    results = cur.fetchall()

    # lists of the runtime, ranks, and movies.
    runtime_list = []
    gross_list = []
    movie_name_list = []
    for res in results:
        runtime_list.append(res[0])
        gross_list.append(res[2])
        movie_name_list.append(res[1])


    cur.execute("SELECT IMDB_Data.IMDB_Votes, IMDB_Data.Movie_Title, MovieChart_2019.Tickets_Sold FROM IMDB_Data LEFT JOIN MovieChart_2019 ON MovieChart_2019.Movie_Title = IMDB_Data.Movie_Title")
    results = cur.fetchall()

    # lists of the runtime, ranks, and movies.
    vote_list = []
    tickets_list = []
    movies_name_list = []
    for res in results:
        vote_list.append(res[0])
        tickets_list.append(res[2])
        movies_name_list.append(res[1])


    # creates a list of the IMDB ratings and IMDB votes of each movie. 
    cur.execute('SELECT IMDB_Rating FROM IMDB_Data')
    results = cur.fetchall()
    rating_on_chart = []
    for tup in results:
        rating_on_chart.append(tup[0])

    
    # creates a scatter plot of each movie's ranking in 2019 that shows the number of favorites and their ranking to see if there is a correlation.
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=rank_on_chart,
        y=movie_names,
        marker=dict(color="rgb(204,255,255)", size=12),
        mode="markers",
        name="Rank on chart",
    ))
    fig2.add_trace(go.Scatter(
        x=favorite_counts,
        y=movie_names,
        marker=dict(color="rgb(0, 153, 153)", size=12),
        mode="markers",
        name="Favorites on Twitter per 100 Tweets",
    ))

    fig2.update_layout(title = "Popularity of movies on Twitter vs. Top Movies 2019 Chart",
                        xaxis_title="Number of favorites/rank", yaxis_title="Movies", xaxis=dict(range=[0, 250]))
    
    fig2.show()

    # creates a scatterplot of comparing runtime of a movie on IMDB versus its gross on the top movies of 2019 chart.
    fig = go.Figure(data=go.Scatter(x=runtime_list,
                                y=gross_list,
                                mode='markers',
                                text=movie_name_list, marker_color = "rgb(153, 0, 153)"))
    fig.update_traces(mode='markers', marker_line_width=2, marker_size=15)
    fig.update_layout(title='Movie Ranking in 2019 vs Runtime on IMDB', xaxis_title="Runtime on IMDB", yaxis_title="Gross in 2019")
    fig.show()
    
    # creates a scatterplot of comparing IMDB votes of a movie on IMDB versus the number of tickets sold on the top 2019 chart.
    fig1 = go.Figure(data=go.Scatter(x=vote_list,
                                y=tickets_list,
                                mode='markers',
                                text=movie_name_list, marker_color = "rgb(230, 230, 250)"))
    fig1.update_traces(mode='markers', marker_line_width=2, marker_size=15)
    fig1.update_layout(title='Number of Tickets Sold on Top Movies 2019 Chart vs IMDB Votes', xaxis_title="IMDB Votes", yaxis_title="Tickets Sold in 2019")
    fig1.show()

    # creates a scatterplot of comparing IMDB ratings of a movie versus its IMDB votes.
    fig3 = go.Figure(data=go.Scatter(x=vote_list,
                                y=rating_on_chart,
                                mode='markers',
                                text=movie_name_list, marker_color = "rgb(128, 0, 32)"))
    fig3.update_traces(mode='markers', marker_line_width=2, marker_size=15)
    fig3.update_layout(title='IMDB Votes vs IMDB Ratings', xaxis_title="IMDB Votes", yaxis_title="IMDB Ratings")
    fig3.show()

    # creates a bar chart comparing the movies favorites on Twitter versus their ranking on the in 2019
    barfig1 = go.Figure([go.Bar(y=favorite_counts, x=movie_name_list)])
    barfig1.update_traces(marker_color="rgb(0, 153, 153)", marker_line_color="rgb(153, 153, 255)", marker_line_width=3, opacity=.7)
    barfig1.update_layout(title_text = "Movie Favorites on Twitter", xaxis_title="Movies in order of Top Movies 2019 Ranking", yaxis_title="Favorites on Twitter per Tweepy search (appx. 100 Tweets)", yaxis=dict(range=[0, 250]))
    barfig1.show()

    # creates a horizontal bar chart comparing the movie followers on Twitter versus its the number of tickets sold on the top 2019 chart.
    fig6 = go.Figure()
    fig6.add_trace(go.Bar(
        y=movie_name_list,
        x=follower_counts,
        name='Twitter Followers',
        orientation='h',
        marker=dict(
            color='rgba(255, 71, 206, 1)',
            line=dict(color='rgba(246, 78, 139, 1.0)', width=3)
        )
    ))
    fig6.add_trace(go.Bar(
        y=movie_name_list,
        x=tickets_list,
        name='Tickets Sold',
        orientation='h',
        marker=dict(
            color='rgba(58, 71, 80, 0.6)',
            line=dict(color='rgba(58, 71, 80, 1.0)', width=3)
        )
    ))

    fig6.update_layout(barmode='stack', title='Movie Followers on Twitter vs. Tickets Sold Per Movie in 2019', xaxis_title="Tickets Sold Per Movie/ Followers on Twitter per Tweepy search (appx. 100 Tweets)", yaxis_title="Movie Titles of Top Grossing Movies of 2019")
    fig6.show()

if __name__ == "__main__":
    main()
