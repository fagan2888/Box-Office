# -*- coding: utf-8 -*-
"""
Created on Sun Jun 17 20:04:33 2018

@author: Rebecca
"""


import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import json
import ast
import re
import math
import movieFunctions as mf
import joinDataModule as jd
import parseColumnsModule as pc
import mergeDataModule as md

# =============================================================================
# ####Main execution file as of now. PART 1.
# ####This first part joins, parses, merges, and de-duplicates our data.
# =============================================================================

# =============================================================================
# #Part 1, join all the datasets together.
# #The joinDataModule requires the location of the each CSV file stored.
# =============================================================================
movies_full_set = jd.joinData()

# =============================================================================
# #Part 2, parse columns.
# #The parseColumns module parses through JSON responses and other 
# #difficult to understand columns.
# =============================================================================
movies_full_set = pc.parseColumns(movies_full_set)

# =============================================================================
# #Part 3, merge columns.
# #The mergeData module merges similar columns obtained from our various 
# #movie sources. 
# =============================================================================
movies_full_set = md.mergeData(movies_full_set)


# =============================================================================
# #Now, begin removing duplicates, step by step
# =============================================================================
#Drop rows completely identical in all columns
movies_full_set =  movies_full_set.iloc[movies_full_set.astype(str).drop_duplicates(keep='first').index]

#Convert movie name to lower case for better comparison for further duplicate testing
movies_full_set['Movie_Name']=movies_full_set['Movie_Name'].str.lower()

#Drop rows with identical movie name and date values only
movies_full_set = movies_full_set.drop_duplicates(subset=['Movie_Name', 'Movie_Date'], keep='first')

#Drop rows with identical movie name and release date within same calendar year
movies_full_set = movies_full_set.sort_values(by=['Movie_Date', 'Movie_Name'], ascending=False)
movies_full_set['Year']=(movies_full_set['Movie_Date']).dt.year
movies_full_set = movies_full_set.drop_duplicates(subset=['Movie_Name', 'Year'], keep='first')


# =============================================================================
# #For our working data set, keep only movies with revenue data
# #or movies released after 2017, since some of the newer
# #movies may not yet have revenues updated in our (mostly) static data sources
# =============================================================================
movies_working_set = movies_full_set[(movies_full_set['Movie_Date']>='2017-01-01')|(movies_full_set['Movie_Revenue']>0)]
movies_working_set.reset_index(drop=True, inplace=True)

#Drop a certain movie manually (doesn't parse correctly in CSV file, and not real theater movie anyway)
#Will think of less-manual way of doing this.
movies_working_set=movies_working_set.drop(movies_working_set.index[641])
movies_working_set.reset_index(drop=True, inplace=True)

# =============================================================================
# #For the purposes of manual entry of missing data, get data ready to split up
# #among team members.
# =============================================================================
#Add a column called 'Delete', where team members will change to '1', if 
#they deem the movie should be dropped because it's either a duplicate
#I did not catch, or a movie that was not released in theaters
movies_working_set['Delete']= 0

movies_2017 = movies_working_set[(movies_working_set['Movie_Date']>='2017-01-01') & \
                             (movies_working_set['Movie_Date']<='2017-12-31')]
movies_2018 = movies_working_set[(movies_working_set['Movie_Date']>='2018-01-01')]
movies_2016 = movies_working_set[(movies_working_set['Movie_Date']>='2016-01-01') & \
                             (movies_working_set['Movie_Date']<='2016-12-31')]
movies_2015_2014 = movies_working_set[(movies_working_set['Movie_Date']>='2014-01-01') & \
                             (movies_working_set['Movie_Date']<='2015-12-31')]
movies_2013_2010 = movies_working_set[(movies_working_set['Movie_Date']>='2010-01-01') & \
                             (movies_working_set['Movie_Date']<='2013-12-31')]
movies_2009_2008 = movies_working_set[(movies_working_set['Movie_Date']>='2008-01-01') & \
                             (movies_working_set['Movie_Date']<='2009-12-31')]

movies_2017.to_csv(r'c:\users\rebecca\desktop\movies_2017.csv')
movies_2018.to_csv(r'c:\users\rebecca\desktop\movies_2018.csv')
movies_2016.to_csv(r'c:\users\rebecca\desktop\movies_2016.csv')
movies_2015_2014.to_csv(r'c:\users\rebecca\desktop\movies_2015_2014.csv')
movies_2013_2010.to_csv(r'c:\users\rebecca\desktop\movies_2013_2010.csv')
movies_2009_2008.to_csv(r'c:\users\rebecca\desktop\movies_2009_2008.csv')

# =============================================================================
# #Count the number of missing values in each important column
# =============================================================================
print("Awards", len(movies_working_set)-(movies_working_set['Awards']).count())
print("Plot", len(movies_working_set)-(movies_working_set['Plot']).count())
print("Rated", len(movies_working_set)-(movies_working_set['Rated']).count())
print("imdbVotes", len(movies_working_set)-(movies_working_set['imdbVotes']).count())
print("RT", len(movies_working_set)-(movies_working_set['Rating_RT']).count())
print("Name", len(movies_working_set)-(movies_working_set['Movie_Name']).count())
print("Revenue", len(movies_working_set)-(movies_working_set['Movie_Revenue'] >0).sum())
print("Date", len(movies_working_set)-(movies_working_set['Movie_Date']).count())
print("Length", len(movies_working_set)-(movies_working_set['Movie_Length'] >0).sum())
print("Budget", len(movies_working_set)-(movies_working_set['Movie_Budget'] >0).sum())
print("Genres", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Genres'] != '[]').sum())
print("Companies", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Companies'] != '[]').sum())
print("Actors", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Actors'] != '[]').sum())
print("keywords", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Keywords'] != '[]').sum())
print("Coll", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Collection'] != '[]').sum())
print("Overview", len(movies_working_set)- (movies_working_set['Movie_Overview']).count())
print("Tagline", len(movies_working_set)- (movies_working_set['Movie_Tagline']).count())
print("Director", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Director'] != '[]').sum())
print("Writer", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Writer'] != '[]').sum())
print("Producer", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Producer'] != '[]').sum())
print("IMDB", len(movies_working_set)- (movies_working_set['Movie_Rating_IMDB']).count())
print("Meta", len(movies_working_set)- (movies_working_set['Movie_Rating_Metacritic']).count())



# =============================================================================
# ####PART 2: This second part modifies our dataframe to make it ready for feeding into models.
# =============================================================================



# =============================================================================
# Commented-out code below gets list of all unique genre types in dataset, if needed.
# Rest of code converts list of genres into boolean-type columns for major genres.
# =============================================================================

#listGenres = list((set.union(*movies_working_set['Movie_Genres'].apply(set).tolist())))
#test=movies_working_set.join(movies_working_set.Movie_Genres.str.join('|').str.get_dummies())
#test[['Action','Adventure','Animation','Biography','Comedy','Crime', \
#       'Documentary','Drama','Family','Fantasy','Foreign','Game-Show', \
#       'History','Horror','Music','Musical','Mystery','News','Reality-TV', \
#       'Romance','Sci-Fi','Science Fiction','Short','Sport','TV Movie', \
#       'Thriller','War','Western']].sum().sort_values()
#test[listGenres].sum().sort_values()
test = movies_working_set.copy(deep=True)
test['Genre_Drama'] = 0
test['Genre_Comedy'] = 0
test['Genre_Action_Adventure'] = 0
test['Genre_Thriller_Horror'] = 0
test['Genre_Romance'] = 0
test['Genre_Crime_Mystery'] = 0
test['Genre_Animation'] = 0
test['Genre_Scifi'] = 0
test['Genre_Documentary'] = 0
test['Genre_Other'] = 0

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


# =============================================================================
# Commented-out code below gets list of all unique Ratings in dataset, if needed.
# Rest of code converts ratings into boolean-type columns.
# =============================================================================

##movies_working_set['Movie_Genres'] = movies_working_set['Movie_Genres'].apply(set)
#listRatings = list(movies_working_set['Rated'].unique())
#movies_working_set.groupby(['Rated']).size()
#
#test = movies_working_set.copy(deep=True)
test['Rated_G_PG'] = 0
test['Rated_PG-13'] = 0
test['Rated_R'] = 0
test['Rated_Other'] = 0

test['Rated_G_PG'] = test['Rated'].apply(mf.makeRatedBoolean, args=('Rated_G_PG',))
test['Rated_PG-13'] = test['Rated'].apply(mf.makeRatedBoolean, args=('Rated_PG-13',))
test['Rated_R'] = test['Rated'].apply(mf.makeRatedBoolean, args=('Rated_R',))
test['Rated_Other'] = test['Rated'].apply(mf.makeRatedBoolean, args=('Rated_Other',))

# =============================================================================
# Limit list of actors to just 5 entries
# =============================================================================

#test['Actors'] = []
test['Actors'] = test['Movie_Actors'].apply(mf.limitNumActors, args=(5,))


# =============================================================================
# Create columns to split up Awards column.
# =============================================================================

listAwards= list(movies_working_set['Awards'].unique())

test['Nominated_Major']=0
test['Won_Major']=0
test['Nominated_Minor']=0
test['Won_Minor']=0

test['Nominated_Major']=test['Awards'].apply(mf.getAwards, args=('majorNod',))
test['Won_Major']=test['Awards'].apply(mf.getAwards, args=('majorWin',))
test['Nominated_Minor']=test['Awards'].apply(mf.getAwards, args=('minorNod',))
test['Won_Minor']=test['Awards'].apply(mf.getAwards, args=('minorWin',))

# =============================================================================
# Commented-out code below gets list of all unique production companies in dataset, if needed
# Rest of code converts list of companies into boolean-type columns for major companies
# =============================================================================

listCompanies = list((set.union(*movies_working_set['Movie_Companies'].apply(set).tolist())))
#test=movies_working_set.join(movies_working_set.Movie_Genres.str.join('|').str.get_dummies())
#test[['Action','Adventure','Animation','Biography','Comedy','Crime', \
#       'Documentary','Drama','Family','Fantasy','Foreign','Game-Show', \
#       'History','Horror','Music','Musical','Mystery','News','Reality-TV', \
#       'Romance','Sci-Fi','Science Fiction','Short','Sport','TV Movie', \
#       'Thriller','War','Western']].sum().sort_values()
#test[listGenres].sum().sort_values()         
        
        
        
        