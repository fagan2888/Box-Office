# -*- coding: utf-8 -*-
"""
Created on Sun Jun 17 20:04:43 2018

@author: Rebecca
"""

import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import json
import ast
import re
import math

# =============================================================================
# The first set of functions are used to parse JSON columns into more 
# easily readable columns of data
# =============================================================================

def getRating(x, source):
    if x is not None and not type(x)==float:
        y=ast.literal_eval(x)
        if source =='Internet Movie Database':          
            for d in y: 
                if d['Source']=='Internet Movie Database':
                    return d['Value']
        if source =='Rotten Tomatoes':          
            for d in y: 
                if d['Source']=='Rotten Tomatoes':
                    return d['Value']                
        if source =='Metacritic':          
            for d in y: 
                if d['Source']=='Metacritic':
                    return d['Value'] 
                                  
def getListGenres(x):
    genreList=[]
    if x is not None and not type(x)==float:
        if not type(x)==str:
            x=str(x)        
        y=ast.literal_eval(x)
        for d in y: 
            genreList.append(d['name'])
    return genreList                
                
def getListCompanies(x):
    compList=[]
    if x is not None and not type(x)==float and not x=='False':#not np.isnan(x):
        if not type(x)==str:
            x=str(x)
        y=ast.literal_eval(x)
        for d in y: 
            compList.append(d['name'])
    return compList  

def getListCollection(x):
    collList=[]
    if x is not None and not type(x)==float and not x=='False':#not np.isnan(x):
        y=ast.literal_eval(x)
        if not type(y)==float:
            collList.append(y['name'])
        
    return collList

def getListCast(x):
    castList=[]
    if x is not None and not type(x)==float and not x=='False':#not np.isnan(x):
        if not type(x)==str:
            x=str(x)
        y=ast.literal_eval(x)
        for d in y: 
            castList.append(d['name'])
    return castList

def getListKeywords(x):
    keywordList=[]
    if x is not None and not type(x)==float and not x=='False':#not np.isnan(x):
        if not type(x)==str:
            x=str(x)
        y=ast.literal_eval(x)
        for d in y: 
            keywordList.append(d['name'])
    return keywordList

def getListLangs(x):
    langList=[]
    if x is not None and not type(x)==float:# and not x=='False':#not np.isnan(x):
        if not type(x)==str:
            x=str(x)
        y=ast.literal_eval(x)
        for d in y: 
            langList.append(d['iso_639_1'])
    return langList

def getListCrew(x, job):
    if x is not None and not type(x)==float:
        y=ast.literal_eval(x)
        if job =='Director':
            dirList=[]
            for person in y:
                if person['job']=='Director':
                    dirList.append(person['name'])
            return dirList        
        if job =='Screenplay':
            wriList=[]
            for person in y:            
                if person['job']=='Screenplay':
                    wriList.append(person['name'])        
            return wriList
        if job =='Producer': 
            prodList=[]
            for person in y:                 
                if person['job']=='Producer':
                    prodList.append(person['name'])
            return prodList
        
def getListWriter(x):
    
    WriterFixed=[]  
    
    if x is not None and not type(x)==float:
        y=re.sub(r' \([^)]*\)', '',x)
        WriterFixed=y.split(", ")
        
    return WriterFixed

# =============================================================================
# This second set of functions are used to merge similar columns among data sources
# together
# =============================================================================
        
def getMovieName(x, columns):
    for name in columns:
        if x[name] is not None and not type(x[name])==float:
            return x[name]
            break

def getMovieRev(x, columns):
    return x[columns].max()

def getMovieDate(x, columns):
    return pd.to_datetime(x[columns], errors='coerce').min()

def getMovieLength(x, columns):
    return x[columns].mean()

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

def getMovieBudget(x, columns):
    return x[columns].mean()

def getMovieID(x, columns):
    for name in columns:
        if type(x[name])==str and not x[name]=='' and not x[name]=='0':
            return int(x[name].strip('tt0'))
        if type(x[name])==float and not math.isnan(x[name]):
            return int(x[name])
            break 
        
def getMovieGenre(x):
    #column 'Genre' is different, it's a string of genres, not a list
    #So, converting to list by splitting on commas, requires checking for nan's.
    genreFixed=[]
    if not type(x['Genre'])==float:
        genreFixed=x['Genre'].split(", ")
    #returning union of items in all genre lists
    #got error without converting entire thing into a string for some reason
    return str(list(set().union(genreFixed, x['genre_x_list'],x['genre_y_list'],x['genre_list'])))        

def getMovieComp(x):
    #column 'Production' is different, it's a string of companies, not a list
    #So, converting to list by splitting on commas, requires checking for nan's.
    compFixed=[]
    if not type(x['Production'])==float:
        compFixed=x['Production'].split(", ")
    #returning union of items in all company lists
    #got error without converting entire thing into a string for some reason
    return str(list(set().union(compFixed, x['production_companies_list'], \
                    x['production_companies_x_list'],x['production_companies_y_list'])))
    
def getMovieCast(x):
    #I want to preserve the ordering in these columns
    #because they seem to be in order of top billing/famous actors.
    #Sets are unordered, so not using same code as in previous section.
    #Using list appends to keep order.
    cast=list(x['cast_x_list'])

    for j in x['cast_y_list']:
        if j not in cast:
            cast.append(j)
    
    if not type(x['Actors'])==float:
        for i in x['Actors'].split(", "):
            if i not in cast:
                cast.append(i)
         
    return str(cast)    

def getMovieKeywords(x):
    #returning union of items in all company lists
    #got error without converting entire thing into a string for some reason
    return str(list(set().union(x['keywords_x_list'],x['keywords_y_list'])))

def getMovieCollection(x):
    #column 'collection.name' is different, it's a string of collections, not a list
    #So, converting to list by splitting on commas, requires checking for nan's.
    collFixed=[]
    if not type(x['belongs_to_collection.name'])==float:
        collFixed=x['belongs_to_collection.name'].split(", ")
    #returning union of items in all collection lists
    #got error without converting entire thing into a string for some reason
    return str(list(set().union(collFixed, x['belongs_to_collection_list'])))

def getMovieOverview(x, columns):
    return x[columns].max()

def getMovieTagline(x, columns):
    return x[columns].max()

def getMovieDirector(x):
    #column 'Director' is different, it's a string, not a list
    #So, converting to list by splitting on commas, requires checking for nan's.
    DirFixed=[]
    if not type(x['Director'])==float:
        DirFixed=x['Director'].split(", ")
    
    #returning union of items in all lists
    #got error without converting entire thing into a string for some reason
    return str(list(set().union(DirFixed, x['crew_x_director'],x['crew_y_director'])))

def getMovieWriter(x):
    return str(list(set().union(x['Writer_fix'], x['crew_x_writer'],x['crew_y_writer'])))

def getMovieProducer(x):
    return str(list(set().union(x['crew_x_producer'],x['crew_y_producer'])))

def getMovieVoteAvg(x):
    #Take weighted average of the vote average columns using
    #vote count columns.  Ignore nan. If total count = 0, also nan.
    count = x[['vote_count', 'vote_count_x', 'vote_count_y']].sum()
    numerator = 0
    #Add to numerator if not nan.
    if (not math.isnan(x['vote_average'])) and (not math.isnan(x['vote_count'])):
        numerator = numerator + (x['vote_count']*x['vote_average'])
    if (not math.isnan(x['vote_average_x'])) and (not math.isnan(x['vote_count_x'])):
        numerator = numerator + (x['vote_count_x']*x['vote_average_x'])
    if (not math.isnan(x['vote_average_y'])) and (not math.isnan(x['vote_count_y'])):
        numerator = numerator + (x['vote_count_y']*x['vote_average_y'])        
        
    #make sure to not divide by zero.
    if count == 0:
        return np.nan
    else: 
        return numerator/count

def getMovieRating(x, source):
    if source=='IMDB':
        if x['Rating_IMDB'] is not None:
            #fixing column; was a string type fraction
            x['Rating_IMDB']=float(x['Rating_IMDB'].split("/")[0])
        else:
            x['Rating_IMDB']=np.nan
        return x[['Rating_IMDB', 'imdbRating']].mean()
 
    if source=='Metacritic':
        if x['Rating_Meta'] is not None:
            #fixing column; was a string type fraction
            x['Rating_Meta']=float(x['Rating_Meta'].split("/")[0])
        else:
            x['Rating_Meta']=np.nan
        return x[['Rating_Meta', 'MetaScore']].mean()
    
def getMoviePop(x, columns):
    return x[columns].mean()


    
    
    
    
    


              