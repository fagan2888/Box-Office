# -*- coding: utf-8 -*-
"""
Created on Sat Jun  9 19:25:29 2018

@author: Rebecca
"""

import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import json
import ast

# =============================================================================
# #Run joinData code first
# 
# =============================================================================

movies_full_test = movies_full.copy(deep=True)

# =============================================================================
# Ratings dicts to columns
# 
# The conditions in the if statements below are necessary
# to take care of some strange data in genre columns before
# sending data to the ast.literal_eval function.
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

movies_full_test['Rating_IMDB']= movies_full_test['Ratings'].apply(getRating, args=('Internet Movie Database',))
movies_full_test['Rating_RT']= movies_full_test['Ratings'].apply(getRating, args=('Rotten Tomatoes',))
movies_full_test['Rating_Meta']= movies_full_test['Ratings'].apply(getRating, args=('Metacritic',))

# =============================================================================
# Genre dicts to list
# 
# The conditions in the if statements below are necessary
# to take care of some strange data in genre columns before
# sending data to the ast.literal_eval function.
# =============================================================================

def getListGenres(x):
    genreList=[]
    if x is not None and not type(x)==float:
        if not type(x)==str:
            x=str(x)        
        y=ast.literal_eval(x)
        for d in y: 
            genreList.append(d['name'])
    return genreList

movies_full_test['genre_x_list'] = movies_full_test['genres_x'].apply(getListGenres)
movies_full_test['genre_y_list'] = movies_full_test['genres_y'].apply(getListGenres)
movies_full_test['genre_list'] = movies_full_test['genres'].apply(getListGenres)

# =============================================================================
# Production companies dicts to list
# 
# Lots of strange/odd data in production company columns.
# All the conditions in the if statements below are necessary
# to take care of them before sending data to 
# the ast.literal_eval function.
# =============================================================================

def getListCompanies(x):
    compList=[]
    if x is not None and not type(x)==float and not x=='False':#not np.isnan(x):
        if not type(x)==str:
            x=str(x)
        y=ast.literal_eval(x)
        for d in y: 
            compList.append(d['name'])
    return compList

movies_full_test['production_companies_x_list']= movies_full_test['production_companies_x'].apply(getListCompanies)
movies_full_test['production_companies_y_list'] = movies_full_test['production_companies_y'].apply(getListCompanies)
movies_full_test['production_companies_list'] = movies_full_test['production_companies'].apply(getListCompanies)

# =============================================================================
# Movie collection/series to list
# 
# Typically just one collection per dictionary, so no looping necessary
# =============================================================================

def getListCollection(x):
    collList=[]
    if x is not None and not type(x)==float and not x=='False':#not np.isnan(x):
        y=ast.literal_eval(x)
        collList.append(y['name'])

    return collList

movies_full_test['belongs_to_collection_list']= movies_full_test['belongs_to_collection'].apply(getListCollection)

# =============================================================================
# Cast dicts to list
# 
# The conditions in the if statements below are necessary
# to take care of them before sending data to 
# the ast.literal_eval function.
# =============================================================================

def getListCast(x):
    castList=[]
    if x is not None and not type(x)==float and not x=='False':#not np.isnan(x):
        if not type(x)==str:
            x=str(x)
        y=ast.literal_eval(x)
        for d in y: 
            castList.append(d['name'])
    return castList

movies_full_test['cast_x_list']= movies_full_test['cast_x'].apply(getListCast)
movies_full_test['cast_y_list'] = movies_full_test['cast_y'].apply(getListCast)


# =============================================================================
# Keywords dicts to list
# 
# The conditions in the if statements below are necessary
# to take care of them before sending data to 
# the ast.literal_eval function.
# =============================================================================

def getListKeywords(x):
    keywordList=[]
    if x is not None and not type(x)==float and not x=='False':#not np.isnan(x):
        if not type(x)==str:
            x=str(x)
        y=ast.literal_eval(x)
        for d in y: 
            keywordList.append(d['name'])
    return keywordList

movies_full_test['keywords_x_list']= movies_full_test['keywords_x'].apply(getListKeywords)
movies_full_test['keywords_y_list'] = movies_full_test['keywords_y'].apply(getListKeywords)
