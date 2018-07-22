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
import re
import movieFunctions as mf

#Comments below reflect how the functions used were constructed.
#To see functions, go to movieFunctions.py

def parseColumns(x):

    # =============================================================================
    # Ratings dicts to columns
    # 
    # The conditions in the if statements of the function below are necessary
    # to take care of some strange data in genre columns before
    # sending data to the ast.literal_eval function.
    # =============================================================================
    
    x['Rating_IMDB']= x['Ratings'].apply(mf.getRating, args=('Internet Movie Database',))
    x['Rating_RT']= x['Ratings'].apply(mf.getRating, args=('Rotten Tomatoes',))
    x['Rating_Meta']= x['Ratings'].apply(mf.getRating, args=('Metacritic',))
    print("Done parsing Ratings")
    # =============================================================================
    # Genre dicts to list
    # 
    # The conditions in the if statements of the function below are necessary
    # to take care of some strange data in genre columns before
    # sending data to the ast.literal_eval function.
    # =============================================================================
    
    x['genre_x_list'] = x['genres_x'].apply(mf.getListGenres)
    x['genre_y_list'] = x['genres_y'].apply(mf.getListGenres)
    x['genre_list'] = x['genres'].apply(mf.getListGenres)
    print("Done parsing Genres")
    # =============================================================================
    # Production companies dicts to list
    # 
    # Lots of strange/odd data in production company columns.
    # All the conditions in the if statements of the function below are necessary
    # to take care of them before sending data to 
    # the ast.literal_eval function.
    # =============================================================================
    
    x['production_companies_x_list']= x['production_companies_x'].apply(mf.getListCompanies)
    x['production_companies_y_list'] = x['production_companies_y'].apply(mf.getListCompanies)
    x['production_companies_list'] = x['production_companies'].apply(mf.getListCompanies)
    print("Done parsing Companies")
    # =============================================================================
    # Movie collection/series to list
    # 
    # Typically just one collection per dictionary, so no looping necessary
    # =============================================================================
    
    x['belongs_to_collection_list']= x['belongs_to_collection'].apply(mf.getListCollection)
    print("Done parsing Collection")
    # =============================================================================
    # Cast dicts to list
    # 
    # The conditions in the if statements of the function below are necessary
    # to take care of them before sending data to 
    # the ast.literal_eval function.
    # =============================================================================
    
    x['cast_x_list']= x['cast_x'].apply(mf.getListCast)
    x['cast_y_list'] = x['cast_y'].apply(mf.getListCast)
    print("Done parsing Cast")

    # =============================================================================
    # Keywords dicts to list
    # 
    # The conditions in the if statements of the function below are necessary
    # to take care of them before sending data to 
    # the ast.literal_eval function.
    # =============================================================================
    
    x['keywords_x_list']= x['keywords_x'].apply(mf.getListKeywords)
    x['keywords_y_list'] = x['keywords_y'].apply(mf.getListKeywords)
    print("Done parsing Keywords")

    # =============================================================================
    # Language dicts to list
    # 
    # =============================================================================
    
    x['spoken_languages_list'] = x['spoken_languages'].apply(mf.getListLangs)
    x['spoken_languages_x_list']= x['spoken_languages_x'].apply(mf.getListLangs)
    x['spoken_languages_y_list'] = x['spoken_languages_y'].apply(mf.getListLangs)
    print("Done parsing Languages")
    # =============================================================================
    # Crew dicts to list
    # 
    # =============================================================================
            
    x['crew_x_director']= x['crew_x'].apply(mf.getListCrew, args=('Director',))
    x['crew_x_director']=x['crew_x_director'].apply(lambda y: [] if y is None else y)
    
    x['crew_x_writer']= x['crew_x'].apply(mf.getListCrew, args=('Screenplay',))
    x['crew_x_writer']=x['crew_x_writer'].apply(lambda y: [] if y is None else y)
    
    x['crew_x_producer']= x['crew_x'].apply(mf.getListCrew, args=('Producer',))
    x['crew_x_producer']=x['crew_x_producer'].apply(lambda y: [] if y is None else y)
    
    x['crew_y_director']= x['crew_y'].apply(mf.getListCrew, args=('Director',))
    x['crew_y_director']=x['crew_y_director'].apply(lambda y: [] if y is None else y)
    
    x['crew_y_writer']= x['crew_y'].apply(mf.getListCrew, args=('Screenplay',))
    x['crew_y_writer']=x['crew_y_writer'].apply(lambda y: [] if y is None else y)
    
    x['crew_y_producer']= x['crew_y'].apply(mf.getListCrew, args=('Producer',))
    x['crew_y_producer']=x['crew_y_producer'].apply(lambda y: [] if y is None else y)
    print("Done parsing Crew")

    # =============================================================================
    # Remove parentheticals in Writer column, convert to list
    # 
    # =============================================================================
                  
    x['Writer_fix']= x['Writer'].apply(mf.getListWriter)
    print("Done parsing Writer")

    return x


