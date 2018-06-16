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
import ast
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
#
#def getMovieLang(x, columns):
#    for name in columns:
#        if x[name] is not None and not type(x[name])==float:
#            return x[name]
#            break 
#
#movies_full_test2['Movie_Lang'] = movies_full_test2.apply(getMovieLang, args=(movie_lang_list_columns,), axis=1)
#movies_full_test2=movies_full_test2.drop(columns=movie_lang_list_columns)

##Only keep rows where each language column is blank or says English is main language
movies_full_test3 = movies_full_test2[
         ((movies_full_test2.original_language=='en') | ((movies_full_test2.original_language).isnull())) & \
         ((movies_full_test2.original_language_x=='en') | ((movies_full_test2.original_language_x).isnull())) & \
         ((movies_full_test2.original_language_y=='en') | ((movies_full_test2.original_language_y).isnull())) & \
        
         ((movies_full_test2.spoken_languages_list.str.len()==0) | (movies_full_test2.spoken_languages_list.str[0]=='en')) & \
         ((movies_full_test2.spoken_languages_x_list.str.len()==0) | (movies_full_test2.spoken_languages_x_list.str[0]=='en')) & \
         ((movies_full_test2.spoken_languages_y_list.str.len()==0) | (movies_full_test2.spoken_languages_y_list.str[0]=='en')) & \
         
         ((movies_full_test2.Language.str.len().isnull()) | (movies_full_test2.Language.str.split(',').str[0]=='English'))]        
        
movies_full_test3=movies_full_test3.drop(columns=movie_lang_list_columns)
movies_full_test3.reset_index(drop=True, inplace=True)


# =============================================================================
# Get union of all genres in all similar/duplicate genre columns
# 
# =============================================================================

movie_genre_list_columns = ['Genre','genre_x_list','genre_y_list', 'genre_list']

def getMovieGenre(x):
    #column 'Genre' is different, it's a string of genres, not a list
    #So, converting to list by splitting on commas, requires checking for nan's.
    genreFixed=[]
    if not type(x['Genre'])==float:
        genreFixed=x['Genre'].split(", ")
    #returning union of items in all genre lists
    #got error without converting entire thing into a string for some reason
    return str(list(set().union(genreFixed, x['genre_x_list'],x['genre_y_list'],x['genre_list'])))


movies_full_test3['Movie_Genres'] = movies_full_test3.apply(getMovieGenre, axis=1)
#comes out as a string, need to convert back into list
movies_full_test3['Movie_Genres'] = movies_full_test3['Movie_Genres'].apply(ast.literal_eval)
movies_full_test3=movies_full_test3.drop(columns=movie_genre_list_columns)


# =============================================================================
# Get union of all production companies in all similar/duplicate company columns
# 
# =============================================================================

movie_comp_list_columns = ['Production','production_companies_list','production_companies_x_list', 'production_companies_y_list']

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


movies_full_test3['Movie_Companies'] = movies_full_test3.apply(getMovieComp, axis=1)
#comes out as a string, need to convert back into list
movies_full_test3['Movie_Companies'] = movies_full_test3['Movie_Companies'].apply(ast.literal_eval)
movies_full_test3=movies_full_test3.drop(columns=movie_comp_list_columns)


# =============================================================================
# Get union of all actors in all similar/duplicate actor columns
# 
# =============================================================================

movie_actor_list_columns = ['Actors','cast_x_list','cast_y_list']

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

movies_full_test3['Movie_Actors'] = movies_full_test3.apply(getMovieCast, axis=1)
#comes out as a string, need to convert back into list
movies_full_test3['Movie_Actors'] = movies_full_test3['Movie_Actors'].apply(ast.literal_eval)
movies_full_test3=movies_full_test3.drop(columns=movie_actor_list_columns)


# =============================================================================
# Get union of all movie keywords in all similar/duplicate keyword columns
# 
# =============================================================================

movie_keyword_list_columns = ['keywords_x_list','keywords_y_list']

def getMovieKeywords(x):
    #returning union of items in all company lists
    #got error without converting entire thing into a string for some reason
    return str(list(set().union(x['keywords_x_list'],x['keywords_y_list'])))


movies_full_test3['Movie_Keywords'] = movies_full_test3.apply(getMovieKeywords, axis=1)
#comes out as a string, need to convert back into list
movies_full_test3['Movie_Keywords'] = movies_full_test3['Movie_Keywords'].apply(ast.literal_eval)
movies_full_test3=movies_full_test3.drop(columns=movie_keyword_list_columns)



# =============================================================================
# Get union of all movie collections in all similar/duplicate collections columns
# 
# =============================================================================

movie_collection_list_columns = ['belongs_to_collection.name','belongs_to_collection_list']

def getMovieCollection(x):
    #column 'collection.name' is different, it's a string of collections, not a list
    #So, converting to list by splitting on commas, requires checking for nan's.
    collFixed=[]
    if not type(x['belongs_to_collection.name'])==float:
        collFixed=x['belongs_to_collection.name'].split(", ")
    #returning union of items in all collection lists
    #got error without converting entire thing into a string for some reason
    return str(list(set().union(collFixed, x['belongs_to_collection_list'])))


movies_full_test3['Movie_Collection'] = movies_full_test3.apply(getMovieCollection, axis=1)
#comes out as a string, need to convert back into list
movies_full_test3['Movie_Collection'] = movies_full_test3['Movie_Collection'].apply(ast.literal_eval)
movies_full_test3=movies_full_test3.drop(columns=movie_collection_list_columns)


# =============================================================================
# Merge similar/duplicate movie overview columns. Simply just keeping the longest
# description.
# =============================================================================

movie_overview_list_columns = ['overview','overview_x','overview_y']

#This is not efficient. But I couldn't get pandas max() function to 
#ignore the nan's. Even though it ignored them in my previous sections
#using the same function.
#As a workaround, I converted all nan's (or floats) to strings.
movies_full_test3['overview'] = movies_full_test3['overview'].apply(lambda x: ' '  if  isfloat(x) is True else x) 
movies_full_test3['overview_x'] = movies_full_test3['overview_x'].apply(lambda x: ' '  if  isfloat(x) is True else x) 
movies_full_test3['overview_y']= movies_full_test3['overview_y'].apply(lambda x: ' '  if  isfloat(x) is True else x) 

def getMovieOverview(x, columns):

    return x[movie_overview_list_columns].max()


movies_full_test3['Movie_Overview'] = movies_full_test3.apply(getMovieOverview, \
                 args=(movie_overview_list_columns,), axis=1)
#converting temp strings ' ' back to nan's
movies_full_test3['Movie_Overview'] = movies_full_test3['Movie_Overview'].apply(lambda x: np.nan  if  x==' ' else x) 

movies_full_test3=movies_full_test3.drop(columns=movie_overview_list_columns)



# =============================================================================
# Merge similar/duplicate movie tagline columns. Simply just keeping the longest
# description.
# =============================================================================

movie_tagline_list_columns = ['tagline','tagline_x','tagline_y']

#This is not efficient. But I couldn't get pandas max() function to 
#ignore the nan's. Even though it ignored them in my previous sections
#using the same function.
#As a workaround, I converted all nan's (or floats) to strings.
movies_full_test3['tagline'] = movies_full_test3['tagline'].apply(lambda x: ' '  if  isfloat(x) is True else x) 
movies_full_test3['tagline_x'] = movies_full_test3['tagline_x'].apply(lambda x: ' '  if  isfloat(x) is True else x) 
movies_full_test3['tagline_y']= movies_full_test3['tagline_y'].apply(lambda x: ' '  if  isfloat(x) is True else x) 

def getMovieTagline(x, columns):

    return x[movie_tagline_list_columns].max()


movies_full_test3['Movie_Tagline'] = movies_full_test3.apply(getMovieTagline, \
                 args=(movie_tagline_list_columns,), axis=1)
#converting temp strings ' ' back to nan's
movies_full_test3['Movie_Tagline'] = movies_full_test3['Movie_Tagline'].apply(lambda x: np.nan  if  x==' ' else x) 

movies_full_test3=movies_full_test3.drop(columns=movie_tagline_list_columns)



