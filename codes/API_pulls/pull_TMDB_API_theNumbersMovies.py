# -*- coding: utf-8 -*-
"""
Created on Sat May 26 13:54:06 2018

@author: Rebecca
"""

import pandas as pd
import json
import requests
import time
from pandas.io.json import json_normalize

#This code will use names/years of movies in The Numbers website
#to do a first run pull from the The Movies DB API (TMDB). 
#TMDB, however, offers more information if you use a tmdb id to pull movie.
#So, I will do a second run pull using the id that I receive from
#first round pull.

#If you want to run this code,
#save the CSV file with movies name and release data info somewhere on your computer.
#Replace this directory with the location where you saved the CSV file.
movies = pd.read_csv(r'c:\users\rebecca\desktop\movies\The Numbers\the_numbers.csv', encoding='latin1')

#Building our API URL. 
#Get your own API Key from the The Movies Database API website
#and replace the alpha-numeric key below with your own.
url_p1 = 'https://api.themoviedb.org/3/search/movie?query='
url_p2 = '&primary_release_year='
api_key = '&api_key=[mykey]'

#Start a count to keep track of where we are (not necessary).
#Create an empty list called "appended_data".
#count=3883
count =0
append_data = []

for item in movies['Movie']:
#for count in range(3883, 5525):movies['Movie'][count]
        response = requests.get(url_p1+item.replace(" ", "+")+url_p2 + \
                   str(pd.to_datetime(movies['Release Date'][count]).year) +api_key)
        data = response.json()
        #Some movies are recorded in the API as having premiered in 2017
        #instead of 2018. So this subtracts 1 from year if results are null
        if 'success' in data and data['success']==False:
            pass
        elif data['total_results'] ==0:
            response = requests.get(url_p1+item.replace(" ", "+")+url_p2 + \
                       str(pd.to_datetime(movies['Release Date'][count]).year-1) +api_key)
            data = response.json()
            append_data.append(data['results'])
            count+=1        
        else:
            append_data.append(data['results'])
            count+=1            
        #This API requres a time delay, so
        #adding 6 second delay after 20 rows pulled        
        if count%20 == 0:
            print("Pulled",count,"rows")
            time.sleep(6)          
        
#        if data['total_results'] ==0:
#            response = requests.get(url_p1+item.replace(" ", "+")+url_p2 + \
#                       str(pd.to_datetime(movies['Release Date'][count]).year-1) +api_key)
#            data = response.json()            
#                
#        append_data.append(data['results'])
#        count+=1

   
    
    
#This API output is a string of information, so we need
#to make it into a readable column format. 
#The JSON_Normalize function helps do this.
#However, going line by line is inefficient, 
#and looking for better ways to do this.
flatten_tmdb_pull_theNumbersMovies = []
for item in range(0,len(append_data)):
    flatten_tmdb_pull_theNumbersMovies.append(json_normalize(append_data[item]))
flatten_tmdb_pull_theNumbersMovies = pd.concat(flatten_tmdb_pull_theNumbersMovies)  
#If you want the dataframe spit out into CSV file, use code below
#and put in your destination.
#flatten_tmdb_pull_newMovies17.to_csv(r'c:\users\rebecca\desktop\flatten_tmdb_pull_newMovies_full.csv')
flatten_tmdb_pull_theNumbersMovies.reset_index(drop=True, inplace=True)


#############################################
#Second run of the API pull will use movie ID.
#Building our API URL again. 
#Get your own API Key from the The Movies Database API website
#and replace the alpha-numeric key below with your own.
url = 'https://api.themoviedb.org/3/movie/'
api_key = '?api_key=61d0a9ceced5fe9582a066af57b751ef'


#Start a count to keep track of where we are (not necessary).
#Create an empty list called "appended_data".
count =0
append_data = []

for item in flatten_tmdb_pull_theNumbersMovies['id']:

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
movies_tmdbapi_full_theNumbers = []
for item in range(0,len(append_data)):
    movies_tmdbapi_full_theNumbers.append(json_normalize(append_data[item]))
movies_tmdbapi_full_theNumbers = pd.concat(movies_tmdbapi_full_theNumbers)
 
movies_tmdbapi_full_theNumbers = movies_tmdbapi_full_theNumbers.drop(columns = ['adult', 'backdrop_path', 'belongs_to_collection', \
           'belongs_to_collection.backdrop_path', 'belongs_to_collection.id', 'belongs_to_collection.poster_path', \
           'homepage', 'poster_path', 'status', 'video'])
#movies_tmdbapi_full_new.to_csv(r'c:\users\rebecca\desktop\movies_tmdbapi_full_new.csv') 
    
#####For some reason, CSV file isn't parsing properly. Using pickle function instead to save data.
movies_tmdbapi_full_theNumbers.to_pickle(r'c:\users\rebecca\desktop\movies_tmdbapi_full_theNumbers')
 
            
            