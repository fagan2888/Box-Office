# -*- coding: utf-8 -*-
"""
Created on Thu May 10 20:22:23 2018

@author: Rebecca
"""

import pandas as pd
import json
import requests
import time
import os

#This code needs the "imdb_ids" of the movies listed in
#the Kaggle dataset here: https://www.kaggle.com/rounakbanik/the-movies-dataset/data,
#in "movies_metadata.csv". 

#Change last line in file to where you want the output file to be saved.

two_up = os.path.abspath(os.path.join(os.getcwd(),"../.."))
path = two_up + '\data\movies_metadata.csv'
movies = pd.read_csv(path)
#Only keep movies that are post 1995
movies_new = movies[movies['release_date']>='1995-01-01']

#Building our API URL. 
#Get your own API Key from the OMDB API website
#and replace the alpha-numeric key below with your own.
#Based on conversations with owner of API, the URL below
#often times out. The direct URL to his server is required
#for this to run without errors. He asked that I not share that
#URL. As a result, what I provide here will likely time out for you.
url = 'http://www.omdbapi.com/?i='
#url = 'http://---.--.---.--/?i='  #can't provide his direct server URL address
api_key = '&apikey=[mykey]'

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
        response = requests.get(url+item+api_key)
        print(count, response)
        data = response.json()
        append_data.append(data)
        count+=1

#If API doesn't time out, and code finishes, this will give us a dataframe with all the data pulled.
omdb_pull = pd.DataFrame(append_data)
#If you want the dataframe spit out into CSV file, use code below
#and put in your destination.
omdb_pull.to_csv(r'c:\users\rebecca\desktop\omdb_pull.csv')
