# -*- coding: utf-8 -*-
"""
Created on Sun May 13 13:41:58 2018

@author: Rebecca
"""

import pandas as pd
import json
import requests
import time
from pandas.io.json import json_normalize

#This code will perform two rounds of pulls from the The Movies Database API website.
#The first round will use imdb ids from another dataset to do the initial pull.
#But we can get more information if we use the API's own id. 
#So the secound round will use another set of ids pulled from the first round.


#First round of pulls.
#This code needs the "imdb_ids" of the movies listed in
#the Kaggle dataset here: https://www.kaggle.com/rounakbanik/the-movies-dataset/data,
#in "movies_metadata.csv". If you want to run this code,
#download that CSV and save it somewhere on your computer.

#Replace this directory with the location where you saved the CSV file.
movies = pd.read_csv(r'c:\users\rebecca\desktop\movies\TheMovies\movies_metadata.csv')
#Only keep movies that are post 1995
movies_new = movies[movies['release_date']>='1995-01-01']

#Building our API URL. 
#Get your own API Key from the The Movies Database API website
#and replace the alpha-numeric key below with your own.
url_p1 = 'https://api.themoviedb.org/3/find/'
url_p2='&language=en-US&external_source=imdb_id'
api_key = '?api_key=61d0a9ceced5fe9582a066af57b751ef'

#Start a count to keep track of where we are (not necessary).
#Create an empty list called "appended_data".
count =0
append_data = []

#Start looping through "imdb_ids" in our dataset
#and create a new URL to feed into API.
#Store the response into variable and append data to list.
for item in movies_new['imdb_id']:
    #Restrict to id's that are strings because
    #there seem to be some incorrect id's not in proper format.
    if type(item) is str:
        response = requests.get(url_p1+item+api_key+url_p2)
        print(count, response)
        data = response.json()
        append_data.append(data['movie_results'])
        count+=1
        #This API requres a time delay, so
        #adding 6 second delay after 30 rows pulled
        if count%30 == 0:
            print("Pulled",count,"rows")
            time.sleep(6)

#This API output is a string of information, so we need
#to make it into a readable column format. 
#The JSON_Normalize function helps do this.
#However, going line by line is inefficient, 
#and looking for better ways to do this.
flatten_tmdb_pull = []
for item in range(0,len(append_data)):
    flatten_tmdb_pull.append(json_normalize(append_data[item]))
flatten_tmdb_pull = pd.concat(flatten_tmdb_pull)  
#If you want the dataframe spit out into CSV file, use code below
#and put in your destination.
flatten_tmdb_pull.to_csv(r'c:\users\rebecca\desktop\flatten_tmdb_pull.csv')

#The Movies Database has their own genre ID table we need to pull.
genre_ids=[]
url_p1='https://api.themoviedb.org/3/genre/movie/list'
url_p2='&language=en-US'
response = requests.get(url_p1+api_key+url_p2)
data = response.json()
genre_ids.append(data['genres'])
genre_ids = json_normalize(genre_ids[0])
#If you want the genre ids spit out into CSV file, use code below
#and put in your destination.
genre_ids.to_csv(r'c:\users\rebecca\desktop\genre_ids.csv')


###############################################################
#Second round of pulls.
#Building our API URL again. 
#Get your own API Key from the The Movies Database API website
#and replace the alpha-numeric key below with your own.
url = 'https://api.themoviedb.org/3/movie/'
api_key = '?api_key=61d0a9ceced5fe9582a066af57b751ef'


#Start a count to keep track of where we are (not necessary).
#Create an empty list called "appended_data".
count =0
append_data = []

for item in movies_tmdbapi['id']:

        response = requests.get(url+str(item) +api_key)
        data = response.json()
        append_data.append(data)#['results'])
        count+=1
        #This API requres a time delay, so
        #adding 6 second delay after 20 rows pulled
        if count%20 == 0:
            print("Pulled",count,"rows")
            time.sleep(6)  
            
            
#This API output is a string of information, so we need
#to make it into a readable column format. 
#The JSON_Normalize function helps do this.
#However, going line by line is inefficient, 
#and looking for better ways to do this.
movies_tmdbapi_full = []
for item in range(0,len(append_data)):
    movies_tmdbapi_full.append(json_normalize(append_data[item]))
movies_tmdbapi_full = pd.concat(movies_tmdbapi_full)   

movies_tmdbapi_full = movies_tmdbapi_full.drop(columns = ['adult', 'backdrop_path', 'belongs_to_collection', \
           'belongs_to_collection.backdrop_path', 'belongs_to_collection.id', 'belongs_to_collection.poster_path', \
           'homepage', 'poster_path', 'status', 'status_code', 'status_message', 'video'])
movies_tmdbapi_full.to_csv(r'c:\users\rebecca\desktop\movies_tmdbapi_full.csv')
            
            


