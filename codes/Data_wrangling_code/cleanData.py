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

####Main execution file as of now. This joins, parses, merges, and de-duplicates our data.

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
# #movies may not yet have revenues updated in our static data sources
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

movies_2017.to_csv(r'c:\users\rebecca\desktop\movies_2017.csv')
movies_2018.to_csv(r'c:\users\rebecca\desktop\movies_2018.csv')




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





