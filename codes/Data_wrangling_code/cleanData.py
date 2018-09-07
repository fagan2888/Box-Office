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
import os
#My own modules:
import movieFunctions as mf
import joinDataModule as jd
import parseColumnsModule as pc
import mergeDataModule as md
import pandas.io.sql as pd_sql
import sqlite3 as sql

# =============================================================================
# ####Main execution file as of now for cleaning data. SECTION 1.
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
print("finished removing most duplicates")

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
print("finished reducing dataset based on lack of revenue")
# =============================================================================
# #For the purposes of manual entry of missing data, get data ready to split up
# #among team members.
# =============================================================================
##Add a column called 'Delete', where team members will change to '1', if 
##they deem the movie should be dropped because it's either a duplicate
##I did not catch, or a movie that was not released in theaters
#movies_working_set['Delete']= 0
#
#movies_2017 = movies_working_set[(movies_working_set['Movie_Date']>='2017-01-01') & \
#                             (movies_working_set['Movie_Date']<='2017-12-31')]
#movies_2018 = movies_working_set[(movies_working_set['Movie_Date']>='2018-01-01')]
#movies_2016 = movies_working_set[(movies_working_set['Movie_Date']>='2016-01-01') & \
#                             (movies_working_set['Movie_Date']<='2016-12-31')]
#movies_2015_2014 = movies_working_set[(movies_working_set['Movie_Date']>='2014-01-01') & \
#                             (movies_working_set['Movie_Date']<='2015-12-31')]
#movies_2013_2010 = movies_working_set[(movies_working_set['Movie_Date']>='2010-01-01') & \
#                             (movies_working_set['Movie_Date']<='2013-12-31')]
#movies_2009_2008 = movies_working_set[(movies_working_set['Movie_Date']>='2008-01-01') & \
#                             (movies_working_set['Movie_Date']<='2009-12-31')]
#movies_2007_1995 = movies_working_set[(movies_working_set['Movie_Date']>='1995-01-01') & \
#                             (movies_working_set['Movie_Date']<='2007-12-31')]

##Send to CSV files for team members:
#movies_2017.to_csv(r'c:\users\rebecca\desktop\movies_2017.csv')
#movies_2018.to_csv(r'c:\users\rebecca\desktop\movies_2018.csv')
#movies_2016.to_csv(r'c:\users\rebecca\desktop\movies_2016.csv')
#movies_2015_2014.to_csv(r'c:\users\rebecca\desktop\movies_2015_2014.csv')
#movies_2013_2010.to_csv(r'c:\users\rebecca\desktop\movies_2013_2010.csv')
#movies_2009_2008.to_csv(r'c:\users\rebecca\desktop\movies_2009_2008.csv')
#movies_2007_1995.to_csv(r'c:\users\rebecca\desktop\movies_2007_1995.csv')


# =============================================================================
# Use if need count of missing values in each important column
# =============================================================================
#print("Awards", len(movies_working_set)-(movies_working_set['Awards']).count())
#print("Plot", len(movies_working_set)-(movies_working_set['Plot']).count())
#print("Rated", len(movies_working_set)-(movies_working_set['Rated']).count())
#print("imdbVotes", len(movies_working_set)-(movies_working_set['imdbVotes']).count())
#print("RT", len(movies_working_set)-(movies_working_set['Rating_RT']).count())
#print("Name", len(movies_working_set)-(movies_working_set['Movie_Name']).count())
#print("Revenue", len(movies_working_set)-(movies_working_set['Movie_Revenue'] >0).sum())
#print("Date", len(movies_working_set)-(movies_working_set['Movie_Date']).count())
#print("Length", len(movies_working_set)-(movies_working_set['Movie_Length'] >0).sum())
#print("Budget", len(movies_working_set)-(movies_working_set['Movie_Budget'] >0).sum())
#print("Genres", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Genres'] != '[]').sum())
#print("Companies", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Companies'] != '[]').sum())
#print("Actors", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Actors'] != '[]').sum())
#print("keywords", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Keywords'] != '[]').sum())
#print("Coll", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Collection'] != '[]').sum())
#print("Overview", len(movies_working_set)- (movies_working_set['Movie_Overview']).count())
#print("Tagline", len(movies_working_set)- (movies_working_set['Movie_Tagline']).count())
#print("Director", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Director'] != '[]').sum())
#print("Writer", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Writer'] != '[]').sum())
#print("Producer", len(movies_working_set)- (movies_working_set.astype(str)['Movie_Producer'] != '[]').sum())
#print("IMDB", len(movies_working_set)- (movies_working_set['Movie_Rating_IMDB']).count())
#print("Meta", len(movies_working_set)- (movies_working_set['Movie_Rating_Metacritic']).count())



# =============================================================================
# ####SECTION 2
# ####This second part collects team members efforts to fill in missing data.
# =============================================================================
two_up = os.path.abspath(os.path.join(os.getcwd(),"../.."))
movies2018_path = two_up + r'\filled_in_data\movies_2018.csv'
movies2017_path = two_up + r'\filled_in_data\movies_2017.csv'
movies2016_path = two_up + r'\filled_in_data\movies_2016.csv'
movies201514_path = two_up + r'\filled_in_data\movies_2015_2014.csv'
movies201310_path = two_up + r'\filled_in_data\movies_2013_2010.csv'
movies200908_path = two_up + r'\filled_in_data\movies_2009_2008.csv'
movies200795_path = two_up + r'\filled_in_data\movies_2007_1995.csv'


movies_2018 = pd.read_csv(movies2018_path, encoding = 'latin-1')
movies_2017 = pd.read_csv(movies2017_path, encoding = 'latin-1')
movies_2016 = pd.read_csv(movies2016_path, encoding = 'latin-1')
movies_2015_2014 = pd.read_csv(movies201514_path, encoding = 'latin-1')
movies_2013_2010 = pd.read_csv(movies201310_path, encoding = 'latin-1')
movies_2009_2008 = pd.read_csv(movies200908_path, encoding = 'latin-1')
movies_2007_1995 = pd.read_csv(movies200795_path, encoding = 'latin-1')


movies_working_set_rebuilt=pd.concat([movies_2018, movies_2017, movies_2016, \
                                      movies_2015_2014, movies_2013_2010, movies_2009_2008, movies_2007_1995])
print("finished rebuilding dataset based on team member efforts to fill in data")
# =============================================================================
# #Fixes human error issues. Mostly stray commas, brackets, semicolons, etc issues.
# =============================================================================
movies_working_set_rebuilt=movies_working_set_rebuilt.drop(columns=['Unnamed: 0'])
movies_working_set_rebuilt.reset_index(drop=True, inplace=True)
#Blank row
movies_working_set_rebuilt.drop(movies_working_set_rebuilt.index[958], inplace=True)
movies_working_set_rebuilt.reset_index(drop=True, inplace=True)
#Actors filled in wrong column
movies_working_set_rebuilt['Movie_Actors'][3138] = movies_working_set_rebuilt['Movie_Companies'][3138]
movies_working_set_rebuilt['Movie_Companies'][3138] = '[]'
#Blank movie collection cell
movies_working_set_rebuilt['Movie_Collection'][259] ='[]'
#Fix genre
movies_working_set_rebuilt['Movie_Genres'][2246] = "['Comedy', 'Horror', 'Sci-Fi']"
movies_working_set_rebuilt['Movie_Genres'][2686] = "['Horror', 'Thriller']"
movies_working_set_rebuilt['Movie_Genres'][2689] = "['Drama', 'History', 'War']"
movies_working_set_rebuilt['Movie_Genres'][2695] = "['Biography', 'Comedy', 'Drama']"
movies_working_set_rebuilt['Movie_Genres'][2761] = "['Action', 'Thriller']"
movies_working_set_rebuilt['Movie_Genres'][2818] = "['Drama', 'History', 'Romance']"
movies_working_set_rebuilt['Movie_Genres'][2832] = "['Action', 'Sport']"
movies_working_set_rebuilt['Movie_Genres'][2998] = "['Crime', 'Drama', 'Thriller']"
movies_working_set_rebuilt['Movie_Genres'][3041] = "['Comedy', 'Thriller']"
movies_working_set_rebuilt['Movie_Genres'][3110] = "['Documentary', 'Comedy']"
movies_working_set_rebuilt['Movie_Genres'][3145] = "['Drama', 'History','War']"
movies_working_set_rebuilt['Movie_Genres'][3169] = "['Action', 'Drama', 'Horror']"
movies_working_set_rebuilt['Movie_Genres'][3203] = "['Drama', 'Romance']"
movies_working_set_rebuilt['Movie_Genres'][3244] = "['Biography', 'Drama', 'History']"

#Fix movie companies
movies_working_set_rebuilt['Movie_Companies'][2121] = "['Quatsous Films', 'Wild Bunch', 'France 2 Cinéma']"
movies_working_set_rebuilt['Movie_Companies'][2245] = "['Final Cut for Real', 'Piraya Film A/S', 'Novaya Zemlya']"
movies_working_set_rebuilt['Movie_Companies'][2831]= "['Eden Films', 'Phoenix Wiley']"
movies_working_set_rebuilt['Movie_Companies'][2916] = "['Reliance Big Pictures', 'Loubyloo Productions', 'Eden Rock Media']"
movies_working_set_rebuilt['Movie_Companies'][3239] = "['EuropaCorp', 'Mad Chance']"

#Fix movie actors
movies_working_set_rebuilt['Movie_Actors'][197] = "['Ava Cooper', 'Ben Foster', 'Christian Bale', 'David Midthunder', 'Gray Wolf Herrera', 'Jesse Plemons', 'John Benjamin Hickey', 'Jonathan Majors', 'Paul Anderson', 'Peter Mullan', 'Qorianka Kilcher', 'Rory Cochrane', 'Rosamund Pike', 'Scott Shepherd', 'Scott Wilson', 'Stella Cooper', 'Stephen Lang', 'Timoth', 'Wes Studi']"
movies_working_set_rebuilt['Movie_Actors'][198] = "['Al Pacino', 'Brittany Snow', 'Chelle Ramos', 'Jermaine Rivers', 'Joe Anderson', 'Jules Haven', 'Karl Urban',' Matt Mercurio', 'Michael Papajohn', 'Odessa Rae', 'Sarah Shahi', 'Steve Coulter', 'Viviana Chavez']"
movies_working_set_rebuilt['Movie_Actors'][880] = "['Jennifer Hale', 'Rapahel Sbarge']"
movies_working_set_rebuilt['Movie_Actors'][905] = "['Cat Brooks', 'Jonathan Cairo', 'Ben McBride','Johnna Watson', 'Sean Whent', 'Juan Carolos Zapta']"
movies_working_set_rebuilt['Movie_Actors'][949] = "['Meryl Steep', 'Tom Hanks', 'Sarah Paulson', ' Bob Odenkirk' , 'Tracy Letts']"
movies_working_set_rebuilt['Movie_Actors'][2556] = "['Ranbir Kapoor', 'Priyanka Chopra', 'Ileana DCruz']"
movies_working_set_rebuilt['Movie_Actors'][2689] = "['Mauricio Kuri', 'Adrian Alonso','Rubén Blades']"
movies_working_set_rebuilt['Movie_Actors'][2695] = "['François Cluzet', 'Omar Sy']"
movies_working_set_rebuilt['Movie_Actors'][2801] = "['Neil Masketll', 'MyAnna Buring', 'Harry Simpson']"
movies_working_set_rebuilt['Movie_Actors'][3041] = "['David Hyde Pierce', 'Clayne Crawford', 'Nathaniel Parker']"
movies_working_set_rebuilt['Movie_Actors'][3112] = "['Abhishek Bachchan', 'Deepika Padukone', 'Bipasha Basu']"
movies_working_set_rebuilt['Movie_Actors'][3127] = "['Michelle Williams', 'Bruce Greenwood', 'Paul Dano']"
movies_working_set_rebuilt['Movie_Actors'][3560] = "['Jennifer Aniston', 'Steve Zahn', 'Woody Harrelson', 'Fred Ward', 'Margo Martindale', 'Kevin Heffernan', 'James Hiroyuki Liao', 'Katie OGrady', 'Yolanda Suarez', 'Don Burns', 'Kimberly Howard', 'Collin Crowley', 'Gilberto Martin del Campo', 'Mark Boone Junior', 'Garfield Wedderburn']"
movies_working_set_rebuilt['Movie_Actors'][3906] = "['Diane Lane', 'Billy Burke', 'Colin Hanks', 'Joseph Cross', 'Mary Beth Hurt', 'Peter Lewis', 'Perla Haney-Jardine', 'Tim DeZam', 'Christopher Cousins', 'Jesse Tyler Ferguson', 'Brynn Baron', 'John Breen', 'Dan Callahan', 'Tyrone Giordano', 'Erin Carufel', 'Ryan Deal', 'Betty Moyer', 'Katie OGrady']"

#Fix movie keywords
movies_working_set_rebuilt['Movie_Keywords'][128] = "['based on novel', 'coma', 'expedition', 'doppleganger', 'lighthouse']"
movies_working_set_rebuilt['Movie_Keywords'][199] = "['fool', 'chump', 'craziness', 'silliness', 'idiot']"
movies_working_set_rebuilt['Movie_Keywords'][206] = "['jungle', 'leg', 'legs', 'pretty legs', 'girl wearing shorts']"
movies_working_set_rebuilt['Movie_Keywords'][267] = "['female frontal nudity', 'dysfunctional family', 'nudity', 'female nudity', 'bare breasts']"
movies_working_set_rebuilt['Movie_Keywords'][283] = "['husband wife relationship', 'inventor', 'wheelchair', 'kenya', 'hospital']"
movies_working_set_rebuilt['Movie_Keywords'][297] = "['wildfire', 'firefighter', 'forest fire', 'natural disaster', 'helicopter']"
movies_working_set_rebuilt['Movie_Keywords'][315] = "['crying', 'flashback', 'snow', 'snowboarding', 'skier']"
movies_working_set_rebuilt['Movie_Keywords'][389] = "['blonde woman', 'lbd', 'older woman younger man sex', 'man checking out womans behind', 'girl wearing a mini skirt']"
movies_working_set_rebuilt['Movie_Keywords'][556] = "['army', 'war', 'adventure']"
movies_working_set_rebuilt['Movie_Keywords'][885] = "['doctor', 'aliens']"
movies_working_set_rebuilt['Movie_Keywords'][949] = "['newspaper', 'pentagon papers', 'whistleblower', '1970s']"
movies_working_set_rebuilt['Movie_Keywords'][2212] = "['ip man character', 'falling off a balcony', 'girl',  'crying',  'starving to death']"
movies_working_set_rebuilt['Movie_Keywords'][2247] = "['movie poster', 'documentary subjects name in title']"
movies_working_set_rebuilt['Movie_Keywords'][2512] = "['strip club', 'c word', 'interrupted sex', 'breaking a bottle over someones head', 'female rear nudity']"
movies_working_set_rebuilt['Movie_Keywords'][2523] = "['slow motion action scene', 'bloody spray', 'extreme blood', 'extreme violence', 'ultra slow motion', 'gore', 'gruesome']"
movies_working_set_rebuilt['Movie_Keywords'][2686] = "['prom', 'escape', 'high school','prom king', 'high school prom']"
movies_working_set_rebuilt['Movie_Keywords'][2689] = "['fanatic', 'arm belt']"
movies_working_set_rebuilt['Movie_Keywords'][2695] = "['class differences', 'black white friendship', 'caregiver', 'paralysis','rich poor']"
movies_working_set_rebuilt['Movie_Keywords'][2778] = "['tree', 'creature', 'forest',  'city', 'walled city']"
movies_working_set_rebuilt['Movie_Keywords'][2834] = "['iran', 'divorce','alzheimers disease', 'court', 'marital problem']"
movies_working_set_rebuilt['Movie_Keywords'][2886] = "['coach', 'basketball', 'league', 'sister']"
movies_working_set_rebuilt['Movie_Keywords'][2897] = "['aunt', 'author', 'confusion', 'abusive father', 'alcoholism']"
movies_working_set_rebuilt['Movie_Keywords'][2906] = "['mauser pistol', 'mauser', 'chinese history']"
movies_working_set_rebuilt['Movie_Keywords'][2947] = "['romantic triangle', 'female nudity', 'menage a trois', 'male nudity']"
movies_working_set_rebuilt['Movie_Keywords'][3081] = "['vomiting','male objectification' , 'male in underwear']"
movies_working_set_rebuilt['Movie_Keywords'][3101] = "['murder', 'revenge', 'fight', 'killing']"
movies_working_set_rebuilt['Movie_Keywords'][3138] = "['Maine','FBI agent', 'supernatural']"
movies_working_set_rebuilt['Movie_Keywords'][3214] = "['hero', 'revenge']"
movies_working_set_rebuilt['Movie_Keywords'][3244] = "['period drama', 'speech therapist', 'speech impediment', 'king george vi', 'king george vi character']"

#Fix movie colleciton
movies_working_set_rebuilt['Movie_Collection'][261] = "['Remake']"
movies_working_set_rebuilt['Movie_Collection'][291]  = "['Tyler Perry', 'Madeas']"
movies_working_set_rebuilt['Movie_Collection'][326] = "['My Little Pony by Hasbro']"
movies_working_set_rebuilt['Movie_Collection'][329] = "['Blade Runner']"
movies_working_set_rebuilt['Movie_Collection'][364] = "['Kingsman']"
movies_working_set_rebuilt['Movie_Collection'][886] = "['Resident Evil collections']"
movies_working_set_rebuilt['Movie_Collection'][2109] = "['night of the living dead']"

#Fix Movie Director
movies_working_set_rebuilt['Movie_Director'][559] = "['Tim Smit']"
movies_working_set_rebuilt['Movie_Director'][888] = "['Ryan Graves']"
movies_working_set_rebuilt['Movie_Director'][2404] = "['Remo DSouza']"
movies_working_set_rebuilt['Movie_Director'][3002] = "['Alex Timbers','Kelly Reichardt']"
movies_working_set_rebuilt['Movie_Director'][3067]= "['Mike Mills']"
movies_working_set_rebuilt['Movie_Director'][3274] = "[]"

#Fix Movie Writer
movies_working_set_rebuilt['Movie_Writer'][2355]  = "['Conor McMahon', 'David OBrien']"
movies_working_set_rebuilt['Movie_Writer'][2404] = "['Amit Aryan', 'Remo DSouza']"
movies_working_set_rebuilt['Movie_Writer'][2778] = "['Dr. Seuss', 'Cinco Paul']"
movies_working_set_rebuilt['Movie_Writer'][3067] = "['Mike Mills']"
movies_working_set_rebuilt['Movie_Writer'][3101] = "['Kaneo Ikegami']"
movies_working_set_rebuilt['Movie_Writer'][3274] = "[]"

#Fix movie producer
movies_working_set_rebuilt['Movie_Producer'][1990] = "['Patrick OBrien', 'Mark Sourian', 'John Gatins']"
movies_working_set_rebuilt['Movie_Producer'][2727] = "['Marianne Gray']"
movies_working_set_rebuilt['Movie_Producer'][2947] = "['Stefan Arndt']"
movies_working_set_rebuilt['Movie_Producer'][3224] = "['Sergey Zernov']"
movies_working_set_rebuilt['Movie_Producer'][3274] = "[]"

movies_working_set_rebuilt.reset_index(drop=True, inplace=True)

##If you want to save to database, use code below:
#con = sql.connect(r'c:\users\rebecca\projs\Box-Office\database\movies.db') 
#movies_working_set_rebuilt.to_sql('cleanedMovies_20180803', con)
#con.commit()
#con.close()


# =============================================================================
# #Discovered issue with 2010 data, uploading missing movies between
# #Jan - Oct 2010
# =============================================================================
two_up = os.path.abspath(os.path.join(os.getcwd(),"../.."))
db_path = two_up + '\database\movies.db'
movies2010_path = two_up + r'\filled_in_data\movies_2010.csv'

con = sql.connect(db_path) 
movies_working_set_rebuilt = pd_sql.read_sql('select * from cleanedMovies_20180803', con, index_col='index')

movies_2010 = pd.read_csv(movies2010_path, encoding = 'latin-1')
movies_2010=movies_2010.drop(columns=['Unnamed: 0'])
movies_2010 = movies_2010[1302:1648]

movies_working_set_rebuilt=pd.concat([movies_working_set_rebuilt, movies_2010])
movies_working_set_rebuilt['Movie_Date']=pd.to_datetime(movies_working_set_rebuilt['Movie_Date'])
movies_working_set_rebuilt = movies_working_set_rebuilt.sort_values(by=['Movie_Date', 'Movie_Name'], ascending=False)
movies_working_set_rebuilt.reset_index(drop=True, inplace=True)

##If you want to save to database, use code below:
#con = sql.connect(r'c:\users\rebecca\projs\Box-Office\database\movies.db') 
#movies_working_set_rebuilt.to_sql('cleanedMovies_20180814', con)
#con.commit()
#con.close()

