# -*- coding: utf-8 -*-
"""
Created on Sun Jun 10 16:55:26 2018

@author: Rebecca
"""

# =============================================================================
# #Run joinData and parseColumns codes first
# 
# =============================================================================

import pandas as pd
import numpy as np
import math

movies_full_test2=movies_full_test.drop(columns=['belongs_to_collection', 'cast_x', 'cast_y','genres', 'genres_x', \
                                'genres_y', 'keywords_x', 'keywords_y','production_companies','production_companies_x', \
                                'production_companies_y','Ratings', 'spoken_languages','spoken_languages_x', \
                                'spoken_languages_y'])

# =============================================================================
# Rename same-named columns
# 
# =============================================================================
column_names = movies_full_test2.columns.values
column_names[32] = 'title_1'
column_names[64] = 'title_2'
movies_full_test2.columns = column_names

# =============================================================================
# Remove similar/duplicate movie title columns
# 
# =============================================================================
movie_name_list_columns = ['Movie', 'original_title','original_title_x','original_title_y','Title','title_x', \
                   'title_1', 'title_y', 'title_2']

def getMovieName(x, columns):
    for name in columns:
        if x[name] is not None and not type(x[name])==float:
            return x[name]
            break 

movies_full_test2['Movie_Name'] = movies_full_test2.apply(getMovieName, args=(movie_name_list_columns,), axis=1)
movies_full_test2=movies_full_test2.drop(columns=movie_name_list_columns)


# =============================================================================
# Merge similar/duplicate movie revenue columns
# 
# =============================================================================

#fix column BoxOffice, there is a cell with the letter 'k', presumably
#indicating in thousand dollars. Just decided to make it nan.
movies_full_test2['BoxOfficeFixed'] = movies_full_test2['BoxOffice']
movies_full_test2['BoxOfficeFixed'] = movies_full_test2['BoxOfficeFixed'].apply(lambda x: \
                 float(x.strip('$').replace(',','')) if  type(x)==str and 'k' not in x else np.nan) 

movie_rev_list_columns = ['BoxOfficeFixed','revenue','revenue_x','revenue_y','Worldwide Gross']

def getMovieRev(x, columns):
    return x[movie_rev_list_columns].max()


movies_full_test2['Movie_Revenue'] = movies_full_test2.apply(getMovieRev, args=(movie_rev_list_columns,), axis=1)

movies_full_test2=movies_full_test2.drop(columns=movie_rev_list_columns)
movies_full_test2=movies_full_test2.drop(columns=['Domestic Gross', 'BoxOffice'])



# =============================================================================
# Pick earliest release date from  similar/duplicate movie date columns
# 
# =============================================================================

movie_date_list_columns = ['Release Date','release_date','release_date_x','release_date_y','Released']

def getMovieDate(x, columns):
    return pd.to_datetime(x[movie_date_list_columns], errors='coerce').min()

movies_full_test2['Movie_Date'] = movies_full_test2.apply(getMovieDate, args=(movie_date_list_columns,), axis=1)

movies_full_test2=movies_full_test2.drop(columns=movie_date_list_columns)



# =============================================================================
# Calculate average runtime from similar/duplicate movie runtime columns
# 
# =============================================================================

#Column Runtime is a string with the word 'min' attached to end. Remove it and make into number.
movies_full_test2['RuntimeFixed'] = movies_full_test2['Runtime'].apply(lambda x: \
                 float(x.strip(' min')) if  type(x)==str else x) 

################CONSIDER INSTANCES IF CELL IS 0 INSTEAD OF NAN?
movie_length_list_columns = ['RuntimeFixed','runtime','runtime_x','runtime_y']
#Don't want zeros to throw off the mean; replace with NaN.
movies_full_test2[movie_length_list_columns] = movies_full_test2[movie_length_list_columns].replace(0, np.NaN)


def getMovieLength(x, columns):
    return x[movie_length_list_columns].mean()

movies_full_test2['Movie_Length'] = movies_full_test2.apply(getMovieLength, args=(movie_length_list_columns,), axis=1)

movies_full_test2=movies_full_test2.drop(columns=movie_length_list_columns)
movies_full_test2=movies_full_test2.drop(columns=['Runtime'])



# =============================================================================
# Calculate average budget from similar/duplicate movie budget columns
# 
# =============================================================================

movie_budget_list_columns = ['budget','budget_x','budget_y','Production Budget']
#Column budget_x seems to be a string instead of number
def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False


#movies_full_test2['budget_x'] = movies_full_test2['budget_x'].apply(lambda x: \
#                 float(x) if  type(x)==str else x) 
movies_full_test2['budget_x'] = movies_full_test2['budget_x'].apply(lambda x: \
                 float(x)  if  isfloat(x) is True else 0) 
#Don't want zeros to throw off the mean; replace with NaN.
movies_full_test2[movie_budget_list_columns] = movies_full_test2[movie_budget_list_columns].replace(0, np.NaN)

def getMovieBudget(x, columns):
    
    return x[movie_budget_list_columns].mean()

movies_full_test2['Movie_Budget'] = movies_full_test2.apply(getMovieBudget, args=(movie_budget_list_columns,), axis=1)
movies_full_test2=movies_full_test2.drop(columns=movie_budget_list_columns)


# =============================================================================
# Get one IMDB ID from similar/duplicate IMDB ID columns
# 
# =============================================================================

movie_imdbid_list_columns = ['imdb_id_x','imdb_id_y','imdbId']

def getMovieID(x, columns):
    for name in columns:
        if type(x[name])==str and not x[name]=='' and not x[name]=='0':
            return int(x[name].strip('tt0'))
        if type(x[name])==float and not math.isnan(x[name]):
            return int(x[name])
            break 


movies_full_test2['Movie_imdb_id'] = movies_full_test2.apply(getMovieID, args=(movie_imdbid_list_columns,), axis=1)
movies_full_test2=movies_full_test2.drop(columns=movie_imdbid_list_columns)


# =============================================================================
# Remove non English movies based on similar/duplicate movie language columns
# 
# =============================================================================
movie_lang_list_columns = ['Language', 'original_language','original_language_x','original_language_y', \
                           'spoken_languages_list','spoken_languages_x_list', 'spoken_languages_y_list']

def getMovieLang(x, columns):
    for name in columns:
        if x[name] is not None and not type(x[name])==float:
            return x[name]
            break 

movies_full_test2['Movie_Lang'] = movies_full_test2.apply(getMovieLang, args=(movie_lang_list_columns,), axis=1)
movies_full_test2=movies_full_test2.drop(columns=movie_lang_list_columns)
