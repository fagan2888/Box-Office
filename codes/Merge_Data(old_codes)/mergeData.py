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
movies_full_test2=movies_full_test.drop(columns=['belongs_to_collection', 'cast_x', 'cast_y', \
                'genres', 'genres_x', 'genres_y', 'keywords_x', 'keywords_y','production_companies', \
                'production_companies_x', 'production_companies_y','Ratings', 'spoken_languages', \
                'spoken_languages_x', 'spoken_languages_y', 'crew_x', 'crew_y', 'Writer', \
                'Domestic Gross', 'Country', 'production_countries','production_countries_x', \
                'production_countries_y'])


# =============================================================================
# Rename same-named columns
# NOT EFFICIENT, BECAUSE NEED TO UPDATE HARDCODED INDEXES WHERE DUPLICATE COLUMNS NAME IS 
# =============================================================================
column_names = movies_full_test2.columns.values
column_names[29] = 'title_1'
column_names[57] = 'title_2'
movies_full_test2.columns = column_names

# =============================================================================
# Remove similar/duplicate movie title columns
# 
# =============================================================================
movie_name_list_columns = ['Movie', 'original_title','original_title_x','original_title_y','Title', \
                           'title_x', 'title_1', 'title_y', 'title_2']

def getMovieName(x, columns):
    for name in columns:
        if x[name] is not None and not type(x[name])==float:
            return x[name]
            break 

movies_full_test2['Movie_Name'] = movies_full_test2.apply(getMovieName, args=(movie_name_list_columns,), axis=1)
movies_full_test2=movies_full_test2.drop(columns=movie_name_list_columns)
print("Done merging Movie Name")

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
    return x[columns].max()


movies_full_test2['Movie_Revenue'] = movies_full_test2.apply(getMovieRev, args=(movie_rev_list_columns,), axis=1)
movies_full_test2=movies_full_test2.drop(columns=movie_rev_list_columns)
movies_full_test2=movies_full_test2.drop(columns=['BoxOffice'])
print("Done merging Movie Revenue")


# =============================================================================
# Pick earliest release date from  similar/duplicate movie date columns
# 
# =============================================================================

movie_date_list_columns = ['Release Date','release_date','release_date_x','release_date_y','Released']

def getMovieDate(x, columns):
    return pd.to_datetime(x[columns], errors='coerce').min()

movies_full_test2['Movie_Date'] = movies_full_test2.apply(getMovieDate, args=(movie_date_list_columns,), axis=1)
movies_full_test2=movies_full_test2.drop(columns=movie_date_list_columns)
print("Done merging Movie Date")

# =============================================================================
# Calculate average runtime from similar/duplicate movie runtime columns
# 
# =============================================================================

#Column Runtime is a string with the word 'min' attached to end. Remove it and make into number.
movies_full_test2['RuntimeFixed'] = movies_full_test2['Runtime'].apply(lambda x: \
                 float(x.strip(' min')) if  type(x)==str else x) 

movie_length_list_columns = ['RuntimeFixed','runtime','runtime_x','runtime_y']
#Don't want zeros to throw off the mean; replace with NaN.
movies_full_test2[movie_length_list_columns] = movies_full_test2[movie_length_list_columns].replace(0, np.NaN)


def getMovieLength(x, columns):
    return x[columns].mean()

movies_full_test2['Movie_Length'] = movies_full_test2.apply(getMovieLength, args=(movie_length_list_columns,), axis=1)
movies_full_test2=movies_full_test2.drop(columns=movie_length_list_columns)
movies_full_test2=movies_full_test2.drop(columns=['Runtime'])
print("Done merging Movie Length")


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


movies_full_test2['budget_x'] = movies_full_test2['budget_x'].apply(lambda x: \
                 float(x)  if  isfloat(x) is True else 0) 
#Don't want zeros to throw off the mean; replace with NaN.
movies_full_test2[movie_budget_list_columns] = movies_full_test2[movie_budget_list_columns].replace(0, np.NaN)

def getMovieBudget(x, columns):
    return x[columns].mean()

movies_full_test2['Movie_Budget'] = movies_full_test2.apply(getMovieBudget, args=(movie_budget_list_columns,), axis=1)
movies_full_test2=movies_full_test2.drop(columns=movie_budget_list_columns)
print("Done merging Movie Budget")

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
print("Done merging Movie IMDB ID")

# =============================================================================
# Remove non English movies based on similar/duplicate movie language columns
# 
# =============================================================================
movie_lang_list_columns = ['Language', 'original_language','original_language_x','original_language_y', \
                           'spoken_languages_list','spoken_languages_x_list', 'spoken_languages_y_list']

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
print("Done dropping non-English movies")

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
print("Done merging Movie Genres")

# =============================================================================
# Get union of all production companies in all similar/duplicate company columns
# 
# =============================================================================

movie_comp_list_columns = ['Production','production_companies_list','production_companies_x_list', \
                           'production_companies_y_list']

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
print("Done merging Movie Companies")

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
print("Done merging Movie Actors")

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
print("Done merging Movie Keywords")


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
print("Done merging Movie Collections")

# =============================================================================
# Merge similar/duplicate movie overview columns. Simply just keeping the longest
# description.
# =============================================================================

movie_overview_list_columns = ['overview','overview_x','overview_y']

#This is not efficient. But I couldn't get pandas max() function to 
#ignore the nan's. Even though it ignored them in my previous sections
#using the same function.
#As a workaround, I converted all nan's (or floats) to strings.
movies_full_test3['overview'] = movies_full_test3['overview'].apply(lambda x: ' '  if \
                 isfloat(x) is True else x) 
movies_full_test3['overview_x'] = movies_full_test3['overview_x'].apply(lambda x: ' '  if \
                 isfloat(x) is True else x) 
movies_full_test3['overview_y']= movies_full_test3['overview_y'].apply(lambda x: ' '  if \
                 isfloat(x) is True else x) 

def getMovieOverview(x, columns):
    return x[columns].max()

movies_full_test3['Movie_Overview'] = movies_full_test3.apply(getMovieOverview, \
                 args=(movie_overview_list_columns,), axis=1)
#converting temp strings ' ' back to nan's
movies_full_test3['Movie_Overview'] = movies_full_test3['Movie_Overview'].apply(lambda x: np.nan  if  x==' ' else x) 
movies_full_test3=movies_full_test3.drop(columns=movie_overview_list_columns)
print("Done merging Movie Overview")


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

    return x[columns].max()


movies_full_test3['Movie_Tagline'] = movies_full_test3.apply(getMovieTagline, \
                 args=(movie_tagline_list_columns,), axis=1)
#converting temp strings ' ' back to nan's
movies_full_test3['Movie_Tagline'] = movies_full_test3['Movie_Tagline'].apply(lambda x: np.nan  if  x==' ' else x) 

movies_full_test3=movies_full_test3.drop(columns=movie_tagline_list_columns)
print("Done merging Movie Tagline")


# =============================================================================
# Merge similar/duplicate movie crew columns. Keeping union of directors,
# writers, and producers.
# =============================================================================

movie_crew_list_columns = ['crew_x_director', 'crew_y_director', 'crew_x_writer', \
                           'crew_y_writer', 'crew_x_producer', 'crew_y_producer', \
                           'Director', 'Writer_fix']

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


movies_full_test3['Movie_Director'] = movies_full_test3.apply(getMovieDirector, axis=1)
#comes out as a string, need to convert back into list
movies_full_test3['Movie_Director'] = movies_full_test3['Movie_Director'].apply(ast.literal_eval)

movies_full_test3['Movie_Writer'] = movies_full_test3.apply(getMovieWriter, axis=1)
movies_full_test3['Movie_Writer'] = movies_full_test3['Movie_Writer'].apply(ast.literal_eval)

movies_full_test3['Movie_Producer'] = movies_full_test3.apply(getMovieProducer, axis=1)
movies_full_test3['Movie_Producer'] = movies_full_test3['Movie_Producer'].apply(ast.literal_eval)

movies_full_test3=movies_full_test3.drop(columns=movie_crew_list_columns)
print("Done merging Movie Crew")


# =============================================================================
# Get average of vote_average columns
# 
# =============================================================================

movie_avg_list_columns = ['vote_average_x', 'vote_count_x', 'vote_average_y', \
                           'vote_count_y', 'vote_average', 'vote_count']

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


movies_full_test3['Movie_VoteAvg'] = movies_full_test3.apply(getMovieVoteAvg, axis=1)
movies_full_test3=movies_full_test3.drop(columns=movie_avg_list_columns)
print("Done merging Movie Average")



# =============================================================================
# Merge IMDB and Metascore rating columns
# 
# =============================================================================

movie_rating_list_columns = ['imdbRating', 'Metascore', 'Rating_IMDB', 'Rating_Meta']

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
        

movies_full_test3['Movie_Rating_IMDB'] = movies_full_test3.apply(getMovieRating, \
                                     args=('IMDB',), axis=1)
movies_full_test3['Movie_Rating_Metacritic'] = movies_full_test3.apply(getMovieRating, \
                                     args=('Metacritic',), axis=1)
movies_full_test3=movies_full_test3.drop(columns=movie_rating_list_columns)
print("Done merging Movie Rating")


# =============================================================================
# Merge popularity columns pulled from the movie database API
# Unsure of how they generate this number; for now, just take average
# =============================================================================

movie_pop_list_columns = ['popularity', 'popularity_x', 'popularity_y']

movies_full_test3['popularity'] = movies_full_test3['popularity'].apply(lambda x: \
                                 float(x)  if type(x)==str else x) 
movies_full_test3['popularity_x'] = movies_full_test3['popularity_x'].apply(lambda x: \
                                 float(x) if type(x)==str else x) 
movies_full_test3['popularity_y'] = movies_full_test3['popularity_y'].apply(lambda x: \
                                 float(x) if type(x)==str else x) 

def getMoviePop(x, columns):
    return x[columns].mean()
        

movies_full_test3['TMDB_popularity'] = movies_full_test3.apply(getMoviePop,args=(movie_pop_list_columns,), axis=1)
movies_full_test3=movies_full_test3.drop(columns=movie_pop_list_columns)
print("Done merging Movie Popularity")