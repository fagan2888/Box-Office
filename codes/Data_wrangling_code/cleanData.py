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
import pandas_datareader as pdr
import movieFunctions as mf
import joinDataModule as jd
import parseColumnsModule as pc
import mergeDataModule as md
#Import production commpany lookup table for later use
companies = pd.read_csv(r'c:\users\rebecca\desktop\movies\companies\production_companies.csv', encoding = 'latin-1')

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
movies_2007_1995 = movies_working_set[(movies_working_set['Movie_Date']>='1995-01-01') & \
                             (movies_working_set['Movie_Date']<='2007-12-31')]

movies_2017.to_csv(r'c:\users\rebecca\desktop\movies_2017.csv')
movies_2018.to_csv(r'c:\users\rebecca\desktop\movies_2018.csv')
movies_2016.to_csv(r'c:\users\rebecca\desktop\movies_2016.csv')
movies_2015_2014.to_csv(r'c:\users\rebecca\desktop\movies_2015_2014.csv')
movies_2013_2010.to_csv(r'c:\users\rebecca\desktop\movies_2013_2010.csv')
movies_2009_2008.to_csv(r'c:\users\rebecca\desktop\movies_2009_2008.csv')
movies_2007_1995.to_csv(r'c:\users\rebecca\desktop\movies_2007_1995.csv')

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

#test[listGenres].sum().sort_values()
test = movies_working_set.copy(deep=True)
#test['Genre_Drama'] = 0
#test['Genre_Comedy'] = 0
#test['Genre_Action_Adventure'] = 0
#test['Genre_Thriller_Horror'] = 0
#test['Genre_Romance'] = 0
#test['Genre_Crime_Mystery'] = 0
#test['Genre_Animation'] = 0
#test['Genre_Scifi'] = 0
#test['Genre_Documentary'] = 0
#test['Genre_Other'] = 0

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
#test['Rated_G_PG'] = 0
#test['Rated_PG-13'] = 0
#test['Rated_R'] = 0
#test['Rated_Other'] = 0

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

#listAwards= list(movies_working_set['Awards'].unique())

#test['Nominated_Major']=0
#test['Won_Major']=0
#test['Nominated_Minor']=0
#test['Won_Minor']=0

test['Nominated_Major']=test['Awards'].apply(mf.getAwards, args=('majorNod',))
test['Won_Major']=test['Awards'].apply(mf.getAwards, args=('majorWin',))
test['Nominated_Minor']=test['Awards'].apply(mf.getAwards, args=('minorNod',))
test['Won_Minor']=test['Awards'].apply(mf.getAwards, args=('minorWin',))

# =============================================================================
# Create boolean to indicate whether movie is part of collection.
# =============================================================================

#test['isCollection'] = 0

test['isCollection'] = test['Movie_Collection'].apply(mf.isCollection)



# =============================================================================
# Create seasonal dummies for release date.
# =============================================================================

#test['Winter']=0
#test['Spring']=0
#test['Summer']=0
#test['Fall']=0
#test['Holiday']=0
        
test['Winter']=test['Movie_Date'].apply(mf.getSeason, args=('Winter',))
test['Spring']=test['Movie_Date'].apply(mf.getSeason, args=('Spring',))
test['Summer']=test['Movie_Date'].apply(mf.getSeason, args=('Summer',))
test['Fall']=test['Movie_Date'].apply(mf.getSeason, args=('Fall',))
test['Holiday']=test['Movie_Date'].apply(mf.getSeason, args=('Holiday',))


# =============================================================================
# Create new columns from revenue and budget columns
# =============================================================================

test['Profit'] = test['Movie_Revenue'] - test['Movie_Budget']

#test['Profit_<1x'] = 0
#test['Profit_[1-2x)'] = 0
#test['Profit_[2-3x)'] = 0
#test['Profit_[3-4x)'] = 0
#test['Profit_[4-5x)'] = 0
#test['Profit_[5x+'] = 0
test['Profit_Bucket']=test.apply(mf.getProfitBucket, axis=1)

test['Profit_<1x'] = test.apply(mf.getProfitBucketBinary, args=('1x',), axis=1)
test['Profit_[1-2x)'] = test.apply(mf.getProfitBucketBinary, args=('2x',), axis=1)
test['Profit_[2-3x)'] = test.apply(mf.getProfitBucketBinary, args=('3x',), axis=1)
test['Profit_[3-4x)'] = test.apply(mf.getProfitBucketBinary, args=('4x',), axis=1)
test['Profit_[4-5x)'] = test.apply(mf.getProfitBucketBinary, args=('5x',), axis=1)
test['Profit_>=5x'] = test.apply(mf.getProfitBucketBinary, args=('5x+',), axis=1)            

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


# =============================================================================
# Commented-out code below gets list of all unique production companies in dataset, if needed
# Rest of code converts list of companies into boolean-type columns for major companies
# =============================================================================

listCompanies = list((set.union(*movies_working_set['Movie_Companies'].apply(set).tolist())))

#test['Comp_Disney'] = 0
#test['Comp_DreamWorks'] = 0
#test['Comp_Fox'] = 0
#test['Comp_Lionsgate'] = 0
#test['Comp_MGM'] = 0
#test['Comp_Miramax'] = 0
#test['Comp_Paramount'] = 0
#test['Comp_Sony'] = 0
#test['Comp_Universal'] = 0
#test['Comp_WarnerBros'] = 0
#test['Comp_Other']=0

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

# =============================================================================
# Short function to fill in any missing plot or overview cells with the other
# if other is not empty.
# Makes it easier to do text analysis with a given text column.
# =============================================================================

test2 = test.copy(deep=True)

test2['Plot']=test2.apply(mf.fillPlot, args=('Movie_Overview', 'Plot',), axis=1)
test2['Movie_Overview']=test2.apply(mf.fillPlot, args=('Plot', 'Movie_Overview',), axis=1)


# =============================================================================
# Sum revenue of movies the listed actors generated in previous movies
# =============================================================================

test['Revenue_Actor'] = float('nan')
test['Revenue_Actor_Real'] = float('nan')
test['Revenue_Director'] = float('nan')
test['Revenue_Director_Real'] = float('nan')
test['Revenue_Producer'] = float('nan')
test['Revenue_Producer_Real'] = float('nan')
#test['Actor_Revenue'] = test.apply(mf.sumActorRevenue)
test=sumRevenue(test, 'Actors', 'Revenue_Actor', 'Revenue_Actor_Real')
test=sumRevenue(test, 'Movie_Director', 'Revenue_Director', 'Revenue_Director_Real')
test=sumRevenue(test, 'Movie_Producer', 'Revenue_Producer', 'Revenue_Producer_Real')

def sumRevenue(data, column, sumColumn, sumColumn_real):
        
    for index in range(len(data)):
        print(index)
        revenue = 0        
        revenue_real = 0
        
        revenueColumn = 'Movie_Revenue'
        revenueColumn_real = 'Revenue_Real'
        
        #dataframe ordered from newest to earliest, so go from index+1 (skip current movie) to last index
        subset = data[index+1:len(data)]
        subset.reset_index(drop=True, inplace=True)
        
        for actor in data[column][index]:       
            for row in range(len(subset)): 
                if actor in subset[column][row]: 
                    if pd.notna(subset[revenueColumn][row]):
                        revenue = revenue + subset[revenueColumn][row]
                        revenue_real = revenue_real + subset[revenueColumn_real][row]
   
        if revenue>0:
            data[sumColumn][index]= revenue
            data[sumColumn_real][index]= revenue_real
                
    

        
#for index in range(len(test)):
#    print(index)
#    revenue = 0
#    revenue_real = 0
#    #get dataframe of only rows prior to current row, so no "index+1"
#    subset = test[index:len(test)]
#    subset.reset_index(drop=True, inplace=True)
#    #print(index, len(test))    
#    for actor in test['Actors'][index]:
#        #print(index, len(test))          
#        for row in range(len(subset)):
#            #print(row, len(subset))  
#            if actor in subset['Actors'][row]:
#                #print(actor)  
#                if pd.notna(subset['Movie_Revenue'][row]):
#                    #print(index, len(test))  
#                    revenue = revenue + subset['Movie_Revenue'][row]
#                    revenue_real = revenue_real + subset['Revenue_Real'][row]
#        
#    if revenue>0:
#        test['Revenue_Actor'][index]= revenue
#        test['Revenue_Actor_Real'][index]= revenue_real
#
##['Olivia Munn', 'Cate Blanchett', 'Dakota Fanning', 'Matt Damon']
#
##test['Revenue_Actor'] = test['Actors'].apply(sumRevenue, args=(test, 'Actors', False,))
##test['Revenue_Actor_Real'] = float('nan')
#test2['Revenue_Actor'] = float('nan')
        
        
        
        
        
        