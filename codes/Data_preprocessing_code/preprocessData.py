# -*- coding: utf-8 -*-
"""
Created on Sun Jul 29 18:55:09 2018

@author: Rebecca
"""


import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import json
import ast
import re
import math
import pandas_datareader as pdr
import pandas.io.sql as pd_sql
import sqlite3 as sql
#My own module:
import movieFunctions as mf
#Import production commpany lookup table for later use
companies = pd.read_csv(r'c:\users\rebecca\desktop\movies\companies\production_companies.csv', encoding = 'latin-1')

# =============================================================================
# This code takes our dataset and modifies some of the text and/or categorical
# and/or list columns into binary columns more usable in machine learning models.
# =============================================================================


# =============================================================================
# Commented-out code below gets list of all unique genre types in dataset, if needed.
# Rest of code converts list of genres into boolean-type columns for major genres.
# =============================================================================

#listGenres = list((set.union(*movies_working_set['Movie_Genres'].apply(set).tolist())))

#test[listGenres].sum().sort_values()
test = movies_working_set.copy(deep=True)

test['Genre_Drama'] = test['Movie_Genres'].apply(mf.makeGenreBoolean, args=('Genre_Drama',))
test['Genre_Comedy'] = test['Movie_Genres'].apply(mf.makeGenreBoolean, args=('Genre_Comedy',))
test['Genre_Action_Adventure'] = test['Movie_Genres'].apply(mf.makeGenreBoolean, args=('Genre_Action_Adventure',))
test['Genre_Thriller_Horror'] = test['Movie_Genres'].apply(mf.makeGenreBoolean, args=('Genre_Thriller_Horror',))
test['Genre_Romance'] = test['Movie_Genres'].apply(mf.makeGenreBoolean, args=('Genre_Romance',))
test['Genre_Crime_Mystery'] = test['Movie_Genres'].apply(mf.makeGenreBoolean, args=('Genre_Crime_Mystery',))
test['Genre_Animation'] = test['Movie_Genres'].apply(mf.makeGenreBoolean, args=('Genre_Animation',))
test['Genre_Scifi'] = test['Movie_Genres'].apply(mf.makeGenreBoolean, args=('Genre_Scifi',))
test['Genre_Documentary'] = test['Movie_Genres'].apply(mf.makeGenreBoolean, args=('Genre_Documentary',))
test['Genre_Other'] = test['Movie_Genres'].apply(mf.makeGenreBoolean, args=('Genre_Other',))

print("Finished encoding genre")

# =============================================================================
# Commented-out code below gets list of all unique Ratings in dataset, if needed.
# Rest of code converts ratings into boolean-type columns.
# =============================================================================

##movies_working_set['Movie_Genres'] = movies_working_set['Movie_Genres'].apply(set)
#listRatings = list(movies_working_set['Rated'].unique())
#movies_working_set.groupby(['Rated']).size()

test['Rated_G_PG'] = test['Rated'].apply(mf.makeRatedBoolean, args=('Rated_G_PG',))
test['Rated_PG-13'] = test['Rated'].apply(mf.makeRatedBoolean, args=('Rated_PG-13',))
test['Rated_R'] = test['Rated'].apply(mf.makeRatedBoolean, args=('Rated_R',))
test['Rated_Other'] = test['Rated'].apply(mf.makeRatedBoolean, args=('Rated_Other',))

print("Finished encoding rating")


# =============================================================================
# Limit list of actors to just 5 entries
# =============================================================================

test['Actors'] = test['Movie_Actors'].apply(mf.limitNumActors, args=(5,))

print("Finished reducing # actors")


# =============================================================================
# Create columns to split up Awards column.
# =============================================================================

#listAwards= list(movies_working_set['Awards'].unique())

test['Nominated_Major']=test['Awards'].apply(mf.getAwards, args=('majorNod',))
test['Won_Major']=test['Awards'].apply(mf.getAwards, args=('majorWin',))
test['Nominated_Minor']=test['Awards'].apply(mf.getAwards, args=('minorNod',))
test['Won_Minor']=test['Awards'].apply(mf.getAwards, args=('minorWin',))

print("Finished encoding awards")


# =============================================================================
# Create boolean to indicate whether movie is part of collection.
# =============================================================================

test['isCollection'] = test['Movie_Collection'].apply(mf.isCollection)

print("Finished encoding collection")


# =============================================================================
# Create seasonal dummies for release date.
# =============================================================================
        
test['Winter']=test['Movie_Date'].apply(mf.getSeason, args=('Winter',))
test['Spring']=test['Movie_Date'].apply(mf.getSeason, args=('Spring',))
test['Summer']=test['Movie_Date'].apply(mf.getSeason, args=('Summer',))
test['Fall']=test['Movie_Date'].apply(mf.getSeason, args=('Fall',))
test['Holiday']=test['Movie_Date'].apply(mf.getSeason, args=('Holiday',))

print("Finished encoding season")


# =============================================================================
# Create new columns from revenue and budget columns
# =============================================================================

test['Profit'] = test['Movie_Revenue'] - test['Movie_Budget']

test['Profit_Bucket']=test.apply(mf.getProfitBucket, axis=1)

test['Profit_<1x'] = test.apply(mf.getProfitBucketBinary, args=('1x',), axis=1)
test['Profit_[1-2x)'] = test.apply(mf.getProfitBucketBinary, args=('2x',), axis=1)
test['Profit_[2-3x)'] = test.apply(mf.getProfitBucketBinary, args=('3x',), axis=1)
test['Profit_[3-4x)'] = test.apply(mf.getProfitBucketBinary, args=('4x',), axis=1)
test['Profit_[4-5x)'] = test.apply(mf.getProfitBucketBinary, args=('5x',), axis=1)
test['Profit_>=5x'] = test.apply(mf.getProfitBucketBinary, args=('5x+',), axis=1)            

print("Finished encoding profit buckets")


# =============================================================================
# Create deflated columns for revenue, budget, profit.
# Profit buckets won't change because I'm dividing rev/budget, so price inflation cancels out
# =============================================================================
#This is using the headline U.S. CPI (all items, urban, seasonally adjusted).
#More research can be done to see whether another inflation metric would be more appropriate
cpi = pdr.fred.FredReader('CPIAUCSL', start='1995-01-01')
cpi = cpi.read()

#Put numbers in June 2018 dollars
jun2018 = cpi[cpi.index=='2018-06-01']['CPIAUCSL'][0]
cpi_baseJun2018 = cpi['CPIAUCSL']/jun2018

test['Revenue_Real'] = test.apply(mf.deflate, args=('Movie_Revenue', cpi_baseJun2018,), axis=1)
test['Budget_Real'] = test.apply(mf.deflate, args=('Movie_Budget', cpi_baseJun2018,), axis=1)
test['Profit_Real'] = test['Revenue_Real']-test['Budget_Real']

print("Finished deflating rev/budget")


# =============================================================================
# Commented-out code below gets list of all unique production companies in dataset, if needed
# Rest of code converts list of companies into boolean-type columns for major companies
# =============================================================================

#listCompanies = list((set.union(*movies_working_set['Movie_Companies'].apply(set).tolist())))

test['Comp_Disney'] = test['Movie_Companies'].apply(mf.getMajorCompanies, args=(companies, 'Disney',))        
test['Comp_DreamWorks'] = test['Movie_Companies'].apply(mf.getMajorCompanies, args=(companies, 'DreamWorks',))        
test['Comp_Fox'] = test['Movie_Companies'].apply(mf.getMajorCompanies, args=(companies, 'Fox',))        
test['Comp_Lionsgate'] = test['Movie_Companies'].apply(mf.getMajorCompanies, args=(companies, 'Lionsgate',))        
test['Comp_MGM'] = test['Movie_Companies'].apply(mf.getMajorCompanies, args=(companies, 'MGM',))        
test['Comp_Miramax'] = test['Movie_Companies'].apply(mf.getMajorCompanies, args=(companies, 'Miramax',))        
test['Comp_Paramount'] = test['Movie_Companies'].apply(mf.getMajorCompanies, args=(companies, 'Paramount',))        
test['Comp_Sony'] = test['Movie_Companies'].apply(mf.getMajorCompanies, args=(companies, 'Sony',))        
test['Comp_Universal'] = test['Movie_Companies'].apply(mf.getMajorCompanies, args=(companies, 'Universal',))        
test['Comp_WarnerBros'] = test['Movie_Companies'].apply(mf.getMajorCompanies, args=(companies, 'Warner Bros Pictures',))        
test['Comp_Other'] = test['Movie_Companies'].apply(mf.getMajorCompanies, args=(companies, 'Other',))        

print("Finished encoding movie company")


# =============================================================================
# Short function to fill in any missing plot or overview cells with the other
# if other is not empty.
# Makes it easier to do text analysis with a given text column.
# =============================================================================

test['Plot']=test.apply(mf.fillPlot, args=('Movie_Overview', 'Plot',), axis=1)
test['Movie_Overview']=test.apply(mf.fillPlot, args=('Plot', 'Movie_Overview',), axis=1)

print("Finished filling up plot/overview")

# =============================================================================
# Sum revenue of movies the listed actors generated in previous movies
# =============================================================================

test['Revenue_Actor'] = float('nan')
test['Revenue_Actor_Real'] = float('nan')
test['Revenue_Director'] = float('nan')
test['Revenue_Director_Real'] = float('nan')
test['Revenue_Writer'] = float('nan')
test['Revenue_Writer_Real'] = float('nan')
test['Revenue_Producer'] = float('nan')
test['Revenue_Producer_Real'] = float('nan')

##test['Actor_Revenue'] = test.apply(mf.sumActorRevenue)
#test=mf.sumRevenue(test, 'Actors', 'Revenue_Actor', 'Revenue_Actor_Real')
#test=mf.sumRevenue(test, 'Movie_Director', 'Revenue_Director', 'Revenue_Director_Real')
#test=mf.sumRevenue(test, 'Movie_Writer', 'Revenue_Writer', 'Revenue_Writer_Real')
#test=mf.sumRevenue(test, 'Movie_Producer', 'Revenue_Producer', 'Revenue_Producer_Real')

print("Finished summing up actor/dir/wri/prod revenues")

        

# =============================================================================
# Save working dataset into SQLite database to start modeling.
# =============================================================================
#Convert all columns of lists into strings of lists and rename to easier names

test[['Movie_Genres', 'Movie_Companies', 'Movie_Actors', 'Movie_Keywords', 'Movie_Collection', \
       'Movie_Director', 'Movie_Writer', 'Movie_Producer','Actors']] = \
       test[['Movie_Genres', 'Movie_Companies', 'Movie_Actors', 'Movie_Keywords', 'Movie_Collection', \
       'Movie_Director', 'Movie_Writer', 'Movie_Producer','Actors']].astype(str)

test.rename(columns={'rating': 'Rating_MovieLens', 'Movie_Name': 'Name', \
                      'Movie_Revenue': 'Revenue', 'Movie_Date': 'Date', \
                      'Movie_Length': 'Length', 'Movie_Budget': 'Budget', \
                      'Movie_imdb_id': 'imdbID', 'Movie_Genres': 'Genres', \
                      'Movie_Companies': 'Companies', 'Movie_Actors': 'Actors_long', \
                      'Movie_Keywords': 'Keywords', 'Movie_Collection': 'Collection', \
                      'Movie_Overview': 'Overview', 'Movie_Tagline': 'Tagline', \
                      'Movie_Director': 'Director', 'Movie_Writer': 'Writer', \
                      'Movie_Producer': 'Producer', 'Movie_Rating_IMDB': 'Rating_IMDB', \
                      'Movie_Rating_Metacritic': 'Rating_Metacritic', 'Actors': 'Actors_short'}, inplace=True)

con = sql.connect('movies.db') 
test.to_sql('movies', con)
con.commit()
con.close()
#train = pd_sql.read_sql('select * from movies4', con, index_col='index')

