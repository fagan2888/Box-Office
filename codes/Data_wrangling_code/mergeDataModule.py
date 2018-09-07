# -*- coding: utf-8 -*-
"""
Created on Sun Jun 10 16:55:26 2018

@author: Rebecca
"""

import pandas as pd
import numpy as np
import math
import ast
import movieFunctions as mf


def mergeData(x):
    
    x=x.drop(columns=['belongs_to_collection', 'cast_x', 'cast_y', \
                    'genres', 'genres_x', 'genres_y', 'keywords_x', 'keywords_y','production_companies', \
                    'production_companies_x', 'production_companies_y','Ratings', 'spoken_languages', \
                    'spoken_languages_x', 'spoken_languages_y', 'crew_x', 'crew_y', 'Writer', \
                    'Domestic Gross', 'Country', 'production_countries','production_countries_x', \
                    'production_countries_y'])


    # =============================================================================
    # Rename same-named columns
    # NOT EFFICIENT, BECAUSE NEED TO UPDATE HARDCODED INDEXES WHERE DUPLICATE COLUMNS NAME IS 
    # =============================================================================
    column_names = x.columns.values
    column_names[10] = 'title_1'
    column_names[29] = 'title_2'
    x.columns = column_names

    # =============================================================================
    # Remove similar/duplicate movie title columns
    # 
    # =============================================================================
    movie_name_list_columns = ['Movie', 'original_title','original_title_x','original_title_y','Title', \
                               'title_x', 'title_1', 'title_y', 'title_2']
    
    x['Movie_Name'] = x.apply(mf.getMovieName, args=(movie_name_list_columns,), axis=1)
    x=x.drop(columns=movie_name_list_columns)
    print("Done merging Movie Name")

    # =============================================================================
    # Merge similar/duplicate movie revenue columns
    # 
    # =============================================================================
    
    #fix column BoxOffice, there is a cell with the letter 'k', presumably
    #indicating in thousand dollars. Just decided to make it nan.
    x['BoxOfficeFixed'] = x['BoxOffice']
    x['BoxOfficeFixed'] = x['BoxOfficeFixed'].apply(lambda y: \
                     float(y.strip('$').replace(',','')) if  type(y)==str and 'k' not in y else np.nan) 
    
    movie_rev_list_columns = ['BoxOfficeFixed','revenue','revenue_x','revenue_y','Worldwide Gross']
    
    x['Movie_Revenue'] = x.apply(mf.getMovieRev, args=(movie_rev_list_columns,), axis=1)
    x=x.drop(columns=movie_rev_list_columns)
    x=x.drop(columns=['BoxOffice'])
    print("Done merging Movie Revenue")


    # =============================================================================
    # Pick earliest release date from  similar/duplicate movie date columns
    # 
    # =============================================================================
    
    movie_date_list_columns = ['Release Date','release_date','release_date_x','release_date_y','Released']
    
    x['Movie_Date'] = x.apply(mf.getMovieDate, args=(movie_date_list_columns,), axis=1)
    x=x.drop(columns=movie_date_list_columns)
    
    x = x[(x['Movie_Date']>='1995-01-01') & (x['Movie_Date']<='2018-06-15')]
    
    print("Done merging Movie Date")

    # =============================================================================
    # Calculate average runtime from similar/duplicate movie runtime columns
    # 
    # =============================================================================
    
    #Column Runtime is a string with the word 'min' attached to end. Remove it and make into number.
    x['RuntimeFixed'] = x['Runtime'].apply(lambda y: \
                     float(y.strip(' min')) if  type(y)==str else y) 
    
    movie_length_list_columns = ['RuntimeFixed','runtime','runtime_x','runtime_y']
    #Don't want zeros to throw off the mean; replace with NaN.
    x[movie_length_list_columns] = x[movie_length_list_columns].replace(0, np.NaN)
    
    x['Movie_Length'] = x.apply(mf.getMovieLength, args=(movie_length_list_columns,), axis=1)
    x=x.drop(columns=movie_length_list_columns)
    x=x.drop(columns=['Runtime'])
    print("Done merging Movie Length")


    # =============================================================================
    # Calculate average budget from similar/duplicate movie budget columns
    # 
    # =============================================================================
    
    movie_budget_list_columns = ['budget','budget_x','budget_y','Production Budget']
    #Column budget_x seems to be a string instead of number
    
    x['budget_x'] = x['budget_x'].apply(lambda y: \
                     float(y)  if  mf.isfloat(y) is True else 0) 
    #Don't want zeros to throw off the mean; replace with NaN.
    x[movie_budget_list_columns] = x[movie_budget_list_columns].replace(0, np.NaN)
    
    x['Movie_Budget'] = x.apply(mf.getMovieBudget, args=(movie_budget_list_columns,), axis=1)
    x=x.drop(columns=movie_budget_list_columns)
    print("Done merging Movie Budget")

    # =============================================================================
    # Get one IMDB ID from similar/duplicate IMDB ID columns
    # 
    # =============================================================================
    
    movie_imdbid_list_columns = ['imdb_id_x','imdb_id_y','imdbId']
    
    x['Movie_imdb_id'] = x.apply(mf.getMovieID, args=(movie_imdbid_list_columns,), axis=1)
    x=x.drop(columns=movie_imdbid_list_columns)
    print("Done merging Movie IMDB ID")

    # =============================================================================
    # Remove non English movies based on similar/duplicate movie language columns
    # 
    # =============================================================================
    movie_lang_list_columns = ['Language', 'original_language','original_language_x','original_language_y', \
                               'spoken_languages_list','spoken_languages_x_list', 'spoken_languages_y_list']
    
    ##Only keep rows where each language column is blank or says English is main language
    x = x[((x.original_language=='en') | ((x.original_language).isnull())) & \
             ((x.original_language_x=='en') | ((x.original_language_x).isnull())) & \
             ((x.original_language_y=='en') | ((x.original_language_y).isnull())) & \
            
             ((x.spoken_languages_list.str.len()==0) | (x.spoken_languages_list.str[0]=='en')) & \
             ((x.spoken_languages_x_list.str.len()==0) | (x.spoken_languages_x_list.str[0]=='en')) & \
             ((x.spoken_languages_y_list.str.len()==0) | (x.spoken_languages_y_list.str[0]=='en')) & \
             
             ((x.Language.str.len().isnull()) | (x.Language.str.split(',').str[0]=='English'))]        
            
    x=x.drop(columns=movie_lang_list_columns)
    x.reset_index(drop=True, inplace=True)
    print("Done dropping non-English movies")

    # =============================================================================
    # Get union of all genres in all similar/duplicate genre columns
    # 
    # =============================================================================
    
    movie_genre_list_columns = ['Genre','genre_x_list','genre_y_list', 'genre_list']
    
    x['Movie_Genres'] = x.apply(mf.getMovieGenre, axis=1)
    #comes out as a string, need to convert back into list
    x['Movie_Genres'] = x['Movie_Genres'].apply(ast.literal_eval)
    x=x.drop(columns=movie_genre_list_columns)
    print("Done merging Movie Genres")

    # =============================================================================
    # Get union of all production companies in all similar/duplicate company columns
    # 
    # =============================================================================
    
    movie_comp_list_columns = ['Production','production_companies_list','production_companies_x_list', \
                               'production_companies_y_list']
    
    x['Movie_Companies'] = x.apply(mf.getMovieComp, axis=1)
    #comes out as a string, need to convert back into list
    x['Movie_Companies'] = x['Movie_Companies'].apply(ast.literal_eval)
    x=x.drop(columns=movie_comp_list_columns)
    print("Done merging Movie Companies")

    # =============================================================================
    # Get union of all actors in all similar/duplicate actor columns
    # 
    # =============================================================================
    
    movie_actor_list_columns = ['Actors','cast_x_list','cast_y_list']          
    
    x['Movie_Actors'] = x.apply(mf.getMovieCast, axis=1)
    #comes out as a string, need to convert back into list
    x['Movie_Actors'] = x['Movie_Actors'].apply(ast.literal_eval)
    x=x.drop(columns=movie_actor_list_columns)
    print("Done merging Movie Actors")

    # =============================================================================
    # Get union of all movie keywords in all similar/duplicate keyword columns
    # 
    # =============================================================================
    
    movie_keyword_list_columns = ['keywords_x_list','keywords_y_list']
    
    x['Movie_Keywords'] = x.apply(mf.getMovieKeywords, axis=1)
    #comes out as a string, need to convert back into list
    x['Movie_Keywords'] = x['Movie_Keywords'].apply(ast.literal_eval)
    x=x.drop(columns=movie_keyword_list_columns)
    print("Done merging Movie Keywords")


    # =============================================================================
    # Get union of all movie collections in all similar/duplicate collections columns
    # 
    # =============================================================================
    
    movie_collection_list_columns = ['belongs_to_collection.name','belongs_to_collection_list']
    
    x['Movie_Collection'] = x.apply(mf.getMovieCollection, axis=1)
    #comes out as a string, need to convert back into list
    x['Movie_Collection'] = x['Movie_Collection'].apply(ast.literal_eval)
    x=x.drop(columns=movie_collection_list_columns)
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
    x['overview'] = x['overview'].apply(lambda y: ' '  if \
                     mf.isfloat(y) is True else y) 
    x['overview_x'] = x['overview_x'].apply(lambda y: ' '  if \
                     mf.isfloat(y) is True else y) 
    x['overview_y']= x['overview_y'].apply(lambda y: ' '  if \
                     mf.isfloat(y) is True else y) 

    x['Movie_Overview'] = x.apply(mf.getMovieOverview, \
                     args=(movie_overview_list_columns,), axis=1)
    #converting temp strings ' ' back to nan's
    x['Movie_Overview'] = x['Movie_Overview'].apply(lambda y: np.nan  if  y==' ' else y) 
    x=x.drop(columns=movie_overview_list_columns)
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
    x['tagline'] = x['tagline'].apply(lambda y: ' '  if  mf.isfloat(y) is True else y) 
    x['tagline_x'] = x['tagline_x'].apply(lambda y: ' '  if  mf.isfloat(y) is True else y) 
    x['tagline_y']= x['tagline_y'].apply(lambda y: ' '  if  mf.isfloat(y) is True else y) 
    
    x['Movie_Tagline'] = x.apply(mf.getMovieTagline, \
                     args=(movie_tagline_list_columns,), axis=1)
    #converting temp strings ' ' back to nan's
    x['Movie_Tagline'] = x['Movie_Tagline'].apply(lambda y: np.nan  if  y==' ' else y) 
    
    x=x.drop(columns=movie_tagline_list_columns)
    print("Done merging Movie Tagline")


    # =============================================================================
    # Merge similar/duplicate movie crew columns. Keeping union of directors,
    # writers, and producers.
    # =============================================================================
    
    movie_crew_list_columns = ['crew_x_director', 'crew_y_director', 'crew_x_writer', \
                               'crew_y_writer', 'crew_x_producer', 'crew_y_producer', \
                               'Director', 'Writer_fix']
    
    x['Movie_Director'] = x.apply(mf.getMovieDirector, axis=1)
    #comes out as a string, need to convert back into list
    x['Movie_Director'] = x['Movie_Director'].apply(ast.literal_eval)
    
    x['Movie_Writer'] = x.apply(mf.getMovieWriter, axis=1)
    x['Movie_Writer'] = x['Movie_Writer'].apply(ast.literal_eval)
    
    x['Movie_Producer'] = x.apply(mf.getMovieProducer, axis=1)
    x['Movie_Producer'] = x['Movie_Producer'].apply(ast.literal_eval)
    
    x=x.drop(columns=movie_crew_list_columns)
    print("Done merging Movie Crew")


    # =============================================================================
    # Get average of vote_average columns
    # 
    # =============================================================================
    
    movie_avg_list_columns = ['vote_average_x', 'vote_count_x', 'vote_average_y', \
                               'vote_count_y', 'vote_average', 'vote_count']
    
    x['Movie_VoteAvg'] = x.apply(mf.getMovieVoteAvg, axis=1)
    x=x.drop(columns=movie_avg_list_columns)
    print("Done merging Movie Average")



    # =============================================================================
    # Merge IMDB and Metascore rating columns
    # 
    # =============================================================================
    
    movie_rating_list_columns = ['imdbRating', 'Metascore', 'Rating_IMDB', 'Rating_Meta']  
    
    x['Movie_Rating_IMDB'] = x.apply(mf.getMovieRating, \
                                         args=('IMDB',), axis=1)
    x['Movie_Rating_Metacritic'] = x.apply(mf.getMovieRating, \
                                         args=('Metacritic',), axis=1)
    x=x.drop(columns=movie_rating_list_columns)
    print("Done merging Movie Rating")


    # =============================================================================
    # Merge popularity columns pulled from the movie database API
    # Unsure of how they generate this number; for now, just take average
    # =============================================================================
    
    movie_pop_list_columns = ['popularity', 'popularity_x', 'popularity_y']
    
#    x['popularity'] = x['popularity'].apply(lambda y: \
#                                     float(y)  if type(y)==str else y) 
#    x['popularity_x'] = x['popularity_x'].apply(lambda y: \
#                                     float(y) if type(y)==str else y) 
#    x['popularity_y'] = x['popularity_y'].apply(lambda y: \
#                                     float(y) if type(y)==str else y) 
    
    
#    x['TMDB_popularity'] = x.apply(mf.getMoviePop,args=(movie_pop_list_columns,), axis=1)
    x['TMDB_popularity'] = x['popularity']
    x=x.drop(columns=movie_pop_list_columns)

    x=x.drop(columns=['homepage', 'id_x', 'status', 'DVD', 'Episode', 'Error', 'Poster', 'Response', 'Season', \
                      'Type', 'Website', 'seriesID', 'totalSeasons', 'id_y'])

    print("Done merging Movie Popularity")
    
    return x

