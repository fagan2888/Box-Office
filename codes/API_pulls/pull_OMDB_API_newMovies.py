# -*- coding: utf-8 -*-
"""
Created on Sat May 26 15:31:47 2018

@author: Rebecca
"""

import pandas as pd
import json
import requests
import time

#This code will use names/years of movies in 2017-mid 2018
#to pull from the themovies db API. If you want to run this code,
#save the CSV file with movies information somewhere on your computer.

#Replace this directory with the location where you saved the CSV file.
movies = pd.read_csv(r'c:\users\rebecca\desktop\movies\2018 Movies\2017_movies_omdb.csv', encoding='latin1')

#Building our API URL. 
#Get your own API Key from the OMDB API website
#and replace the alpha-numeric key below with your own.
#Based on conversations with owner of API, the URL below
#often times out. The direct URL to his server is required
#for this to run without errors. He asked that I not share that
#URL. As a result, what I provide here will likely time out for you.
#url = 'http://www.omdbapi.com/?t='
url_p1 = 'http://---.--.---.--/?t='  #can't provide his direct server URL address
url_p2 = '&y='
api_key = '&apikey=[mykey]'

#Start a count to keep track of where we are (not necessary).
#Create an empty list called "appended_data".
count =0
append_data = []

#Start looping through "imdb_ids" in our dataset
#and create a new URL to feed into API.
#Store the response into variable and append data to list.
for item in movies['movie']:#[350:]:

    response = requests.get(url_p1+item.replace(" ", "+")+ \
               url_p2 + str(pd.to_datetime(movies['date'][count]).year)+\
               api_key)
    print(count, response)
    data = response.json()
    
    if data['Response']=='False':
        response = requests.get(url_p1+item.replace(" ", "+")+ \
               url_p2 + str(pd.to_datetime(movies['date'][count]).year-1)+\
               api_key)
        data = response.json()
        
    append_data.append(data)
    count+=1

#If API doesn't time out, and code finishes, this will give us a dataframe with all the data pulled.
omdb_pull_newMovies17 = pd.DataFrame(append_data)
#If you want the dataframe spit out into CSV file, use code below
#and put in your destination.
omdb_pull_newMovies17.to_csv(r'c:\users\rebecca\desktop\omdb_pull_newMovies17.csv')
