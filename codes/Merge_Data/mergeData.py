# -*- coding: utf-8 -*-
"""
Created on Fri Jun  1 15:03:55 2018

@author: Rebecca
"""


import pandas as pd
import numpy as np
import json
import requests
import datetime
from datetime import datetime

import time
import sqlite3
import matplotlib.pyplot as plt
from pandas.io.json import json_normalize


#This code needs information from Kaggle dataset here: https://www.kaggle.com/rounakbanik/the-movies-dataset/data,
#If you want to run this code, download that CSV and save it somewhere on your computer.
#Replace this directory with the location where you saved the CSV file.
movies = pd.read_csv(r'c:\users\rebecca\desktop\movies\TheMovies\movies_metadata.csv')
ratings = pd.read_csv(r'c:\users\rebecca\desktop\movies\TheMovies\ratings.csv')
movie_credits = pd.read_csv(r'c:\users\rebecca\desktop\movies\TheMovies\credits.csv')
keywords = pd.read_csv(r'c:\users\rebecca\desktop\movies\TheMovies\keywords.csv')
id_lookup= pd.read_csv(r'c:\users\rebecca\desktop\movies\TheMovies\links.csv')
print("Done loading Kaggle data part 1")

#This code needs information from Kaggle dataset here: https://www.kaggle.com/tmdb/tmdb-movie-metadata/data,
#If you want to run this code, download that CSV and save it somewhere on your computer.
movies_tmdb = pd.read_csv(r'c:\users\rebecca\desktop\movies\TMDB\tmdb_5000_movies.csv')
credits_tmdb = pd.read_csv(r'c:\users\rebecca\desktop\movies\TMDB\tmdb_5000_credits.csv')
print("Done loading Kaggle data part 2")

#This code needs data pulled from OMDB API and TMDB API
movies_omdbapi = pd.read_csv(r'c:\users\rebecca\desktop\movies\API\omdb_pull.csv')
movies_tmdbapi = pd.read_csv(r'c:\users\rebecca\desktop\movies\API\flatten_tmdb_pull.csv')
genre_tmdbapi = pd.read_csv(r'c:\users\rebecca\desktop\movies\API\genre_ids.csv')
print("Done loading API data")

#This code needs data pulled from the-numbers.com
the_numbers = pd.read_csv(r'c:\users\rebecca\desktop\movies\the numbers\the_numbers.csv', encoding='latin1')


#Only keep movies that are post 1995
movies_new = movies[movies['release_date']>='1995-01-01']
the_numbers_new = the_numbers[(the_numbers['Release Date']>='1995-01-01') & (the_numbers['Release Date']<='2018-06-15')]
#Group ratings by movie id
ratings_grp = ratings.groupby('movieId')[['rating']].mean().reset_index()


#Start joining files from https://www.kaggle.com/rounakbanik/the-movies-dataset/data
kaggle_pt1 = pd.merge(movies_new, id_lookup, how='left', left_on="id", \
                             right_on=id_lookup['tmdbId'].fillna(0).astype(int).astype(str))
kaggle_pt1 = pd.merge(kaggle_pt1, movie_credits, how="left", left_on = "tmdbId", \
                             right_on = "id")
kaggle_pt1 = pd.merge(kaggle_pt1, ratings_grp, how="left", left_on = "movieId", \
                             right_on = "movieId")
kaggle_pt1 = pd.merge(kaggle_pt1, keywords, how="left", left_on = "tmdbId", \
                             right_on = "id")
kaggle_pt1=kaggle_pt1.drop(columns=['id_x', 'id_y', 'id','adult', 'homepage', \
                                'poster_path', 'status', 'video',])
print("Done joining Kaggle data part 1")

#Start joining files from https://www.kaggle.com/tmdb/tmdb-movie-metadata/data
kaggle_full = pd.merge(kaggle_pt1, movies_tmdb, how="outer", left_on="tmdbId", right_on="id")
kaggle_full = kaggle_full[(kaggle_full['release_date_y']>='1995-01-01') | \
                          (kaggle_full['release_date_x']>='1995-01-01')]
kaggle_full = pd.merge(kaggle_full, credits_tmdb, how="left", left_on="tmdbId", right_on="movie_id")
print("Done joining Kaggle data part 2")

#Start joining files fro OMDB API and TMDB API
kaggle_plus_api = pd.merge(kaggle_full, movies_omdbapi, how="left", left_on='imdb_id', right_on = 'imdbID')
kaggle_apis = pd.merge(kaggle_plus_api, movies_tmdbapi, how="left", left_on='tmdbId', right_on='id')
test=kaggle_apis.drop(columns=['homepage','id_x', 'movie_id', 'Unnamed: 0_x','DVD','Poster', 'Response', \
                               'Season','type','website', 'imdbID', 'seriesID','totalSeasons','Unnamed: 0_y', \
                               'adult','backdrop_path','id_y', 'poster_path', 'video'])
test = test.drop(columns = ['id_y', 'backdrop_path', 'Website', 'Error', 'Episode', 'Type', 'status', 'imdb_id'])
test = test.drop_duplicates(keep='first')

#################################################3
#Join the datasets for newer movies
#stillneed to do TMDB properly. New movie CSV file not properly parsed, so can't load
#and still need to rerun all of them to pull full attributes









######################################################
#Join data from the-numbers.com
#First restrict sample period
#Make movie titles lower case
#NEED TO also join on, at least, YEAR. There will be wrong, duplicates otherwise
the_numbers['Release Date'] = pd.to_datetime(the_numbers['Release Date'], format='%m/%d/%Y')
the_numbers = the_numbers[(the_numbers['Release Date']>='1995-01-01') & \
                          (the_numbers['Release Date']<='2018-06-15')]
the_numbers.reset_index(drop=True, inplace=True)
test7=test
test7['original_title'] = test7['original_title'].str.lower()
the_numbers['Movie'] = the_numbers['Movie'].str.lower()

test8=pd.merge(the_numbers, test7, how="outer", left_on=['Movie', the_numbers['Release Date'].dt.year], right_on=['original_title',pd.to_datetime(test7['release_date']).dt.year])

#######################################################



