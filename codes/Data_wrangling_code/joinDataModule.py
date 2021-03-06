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
import zipfile

import os
import time
import sqlite3
import matplotlib.pyplot as plt
from pandas.io.json import json_normalize

# =============================================================================
# #File directories may need to be updated based on where files are stored on your computer
# #However, these paths may work without needing to be updated.
# =============================================================================
two_up = os.path.abspath(os.path.join(os.getcwd(),"../.."))
movies_path = two_up + '\data\movies_metadata.csv'
ratings_path = two_up + '\data\\ratings.zip'
credits_path = two_up + '\data\credits.zip'
keywords_path = two_up + '\data\keywords.csv'
links_path = two_up + '\data\links.csv'
tmovies_path = two_up + '\data\\tmdb_5000_movies.csv'
tcredits_path = two_up + '\data\\tmdb_5000_credits.csv'
omdb_path = two_up + '\data\omdb_pull.csv'
tmdb_path = two_up + '\data\movies_tmdbapi_full.csv'
tpickle_path = two_up + '\data\movies_tmdbapi_full_new'
tpicklenew_path = two_up + '\data\movies_tmdbapi_full_theNumbers'
thenum_path = two_up + '\data\\the_numbers.csv'

def joinData():
    
    # =============================================================================
    # #This code needs information from Kaggle dataset here: https://www.kaggle.com/rounakbanik/the-movies-dataset/data,
    # =============================================================================
    movies = pd.read_csv(movies_path)
    zf = zipfile.ZipFile(ratings_path)
    ratings = pd.read_csv(zf.open('ratings.csv'))
    zf = zipfile.ZipFile(credits_path)
    movie_credits = pd.read_csv(zf.open('credits.csv'))
    keywords = pd.read_csv(keywords_path)
    id_lookup= pd.read_csv(links_path)
    print("Done loading Kaggle data part 1")
    
    
    # =============================================================================
    # #This code needs information from Kaggle dataset here: https://www.kaggle.com/tmdb/tmdb-movie-metadata/data,
    # =============================================================================
    movies_tmdb = pd.read_csv(tmovies_path)
    credits_tmdb = pd.read_csv(tcredits_path)
    print("Done loading Kaggle data part 2")
    
    
    # =============================================================================
    # #This code needs data pulled from OMDB API and TMDB API
    # =============================================================================
    movies_omdbapi = pd.read_csv(omdb_path)
    movies_tmdbapi = pd.read_csv(tmdb_path)
    movies_tmdbapi=movies_tmdbapi.drop(columns=['Unnamed: 0'])
    #genre_tmdbapi = pd.read_csv(r'c:\users\rebecca\desktop\movies\API\genre_ids.csv')
    movies_tmdbapi_new =pd.DataFrame(pd.read_pickle(tpickle_path))
    movies_tmdbapi_theNumbers =pd.DataFrame(pd.read_pickle(tpicklenew_path))    
    movies_tmdbapi = movies_tmdbapi.append(movies_tmdbapi_new)
    movies_tmdbapi = movies_tmdbapi.append(movies_tmdbapi_theNumbers)
    
    print("Done loading API data")
    
    
    # =============================================================================
    # #This code needs data pulled from the-numbers.com
    # =============================================================================
    the_numbers = pd.read_csv(thenum_path, encoding='latin1')
    print("Done loading The Numbers data")
    
    
    # =============================================================================
    # #Only keep movies that are post 1995
    # =============================================================================
    movies_new = movies[movies['release_date']>='1995-01-01']
    movies_new.reset_index(drop=True, inplace=True)
    
    # =============================================================================
    # #Group ratings by movie id
    # =============================================================================
    ratings_grp = ratings.groupby('movieId')[['rating']].mean().reset_index()
    print("Done keeping only post 1995 movies")
    
    
    # =============================================================================
    # #Start joining files from https://www.kaggle.com/rounakbanik/the-movies-dataset/data
    # =============================================================================
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
    
    
    # =============================================================================
    # #Start joining files from https://www.kaggle.com/tmdb/tmdb-movie-metadata/data
    # =============================================================================
    kaggle_full = pd.merge(kaggle_pt1, movies_tmdb, how="outer", left_on="tmdbId", right_on="id")
    kaggle_full = kaggle_full[(kaggle_full['release_date_y']>='1995-01-01') | \
                              (kaggle_full['release_date_x']>='1995-01-01')]
    kaggle_full = pd.merge(kaggle_full, credits_tmdb, how="left", left_on="tmdbId", right_on="movie_id")
    print("Done joining Kaggle data part 2")
    
    
    # =============================================================================
    # #Start joining files from OMDB API and TMDB API
    # =============================================================================
    kaggle_plus_api = pd.merge(kaggle_full, movies_omdbapi, how="outer", left_on='imdb_id', right_on = 'imdbID')
    kaggle_apis = pd.merge(kaggle_plus_api, movies_tmdbapi, how="outer", left_on='tmdbId', right_on='id')
    
    #kaggle_apis=kaggle_apis.drop(columns=['homepage','id_x', 'movie_id', 'Unnamed: 0','DVD','Poster', 'Response', \
    #                               'Season','Type','Website', 'imdbID', 'seriesID','totalSeasons', \
    #                               'id_y'])
    #kaggle_apis = kaggle_apis.drop(columns = ['Error', 'Episode',  'status'])
    
    kaggle_apis = kaggle_apis.iloc[kaggle_apis.astype(str).drop_duplicates(keep='first').index]
    kaggle_apis.reset_index(drop=True, inplace=True)
    
    print("Done joining APIs")
    
    
    # =============================================================================
    # #Join data from the-numbers.com
    # #First restrict sample period:
    # =============================================================================
    the_numbers['Release Date'] = pd.to_datetime(the_numbers['Release Date'], format='%m/%d/%Y')
    the_numbers = the_numbers[(the_numbers['Release Date']>='1995-01-01') & \
                              (the_numbers['Release Date']<='2018-06-15')]
    the_numbers.reset_index(drop=True, inplace=True)
    
    #Need to put titles in lower case to more easily join them.
    kaggle_apis['original_title'] = kaggle_apis['original_title'].str.lower()
    the_numbers['Movie'] = the_numbers['Movie'].str.lower()
    
    kaggle_apis['release_date_year'] = pd.to_datetime(kaggle_apis['release_date']).dt.year
    the_numbers['Release Date year'] = the_numbers['Release Date'].dt.year
    
    #Joining using movie name and year of release
    movies_full=pd.merge(kaggle_apis,the_numbers, how="outer", \
                  left_on=['original_title','release_date_year'], \
                  right_on=['Movie', 'Release Date year'])
    
    movies_full = movies_full.drop(columns = ['Unnamed: 0', 'release_date_year', 'Release Date year'])
    
    movies_full = movies_full.iloc[movies_full.astype(str).drop_duplicates(keep='first').index]
    
    movies_full.reset_index(drop=True, inplace=True)
    
    print("Done joining The Numbers data")
    
    return movies_full
    
