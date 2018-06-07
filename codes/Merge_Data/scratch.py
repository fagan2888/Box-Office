# -*- coding: utf-8 -*-
"""
Created on Mon May 28 18:46:31 2018

@author: Rebecca
"""


#ratings['timestamp']=datetime.datetime.fromtimestamp(ratings['timestamp']).strftime('%Y-%m-%d').values
#ratings['timestamp']=datetime.date(pd.to_datetime(ratings['timestamp']).year, \
#       pd.to_datetime(ratings['timestamp']).month, pd.to_datetime(ratings['timestamp']).day)
#ratings['timestamp']=pd.to_datetime(ratings['timestamp'])
ratings_grp = ratings.groupby('movieId')[['rating']].mean().reset_index()
#plt.hist(df['rating'])
#rating id is movieID, keywords id is tmdbID, movies id is tmdbid BUT as STRING, crdits is tmdbID
#df2=pd.merge(movie_credits, id_lookup, how='inner',left_on="id", right_on="tmdbId")# right_on=id_lookup['tmdbId'].fillna(0).astype(int).astype(str))


The list of dictionaries in many columns are actually strings.  In order to get them into a dataframe-type object,
need to decode using json.loads function. Json.loads needs double quotes and only understands "null" for empty
cells.  

So, first I need to replace single quotes with double quotes.
Then for any cells with nan, need to convert nan to string, because nan is float.
After nan is string, replace nan with null.
Then apply json.loads to entire column.
Then you can apply a dataframe to the result.

The problem is that the resulting dataframe make the Key and Value into the columns.
Instead, I need the actual keys to be their own column, and the values to be in the row.

So I try transposing the dataframe and resetting the index.  Then append it to larger
dataframe. Generally works until I get to a null row. I am basically skipping null rows,
so will affect overall movie dataframe.


test2=test[0:5000]
pd.options.mode.chained_assignment = None
test2['Ratings'] = test2['Ratings'].str.replace("'", "\"")
test2['Ratings'] = test2['Ratings'].apply(lambda x: str(x) if pd.isnull(x) else x)
test2['Ratings']= test2['Ratings'].str.replace("nan", "null")
test2['Ratings']= test2['Ratings'].apply(json.loads)
test2.reset_index(drop=True, inplace=True)

flatten_ratings = []
for item in range(0,5000):
    if test2['Ratings'][item] is not None: 
        flatten_ratings.append(pd.DataFrame(test2['Ratings'][item]).set_index('Source').T)
        
flatten_ratings = pd.concat(flatten_ratings)  
flatten_ratings.reset_index(drop=True, inplace=True)




Step 1: do a string replace of single quotes for double quotes in these list of dicts
test3= test['Ratings'].str.replace("'", "\"")

Step 2: do a replace of nan with Strings nan
test3= test3.apply(lambda x: str(x) if pd.isnull(x) else x)

Step 2a: replace string nan with null
test3= test3.str.replace("nan", "null")

Step 3: then do apply json.loads
test6 = test5['Ratings'].apply(json.loads)

Step 3: then convert to dataframe...invert
BUT how to do this for entire column...iterate over each row and append?
pd.DataFrame(json.loads(test5['Ratings'][0])).set_index('Source').T



test3= test['Ratings'].str.replace("'", "\"")
test4 = json.loads(test3)
test4.apply(json.loads)
json.loads(str(test3[30311]))
test3= test3.apply(lambda x: str(x) if pd.isnull(x) else x)

test5=pd.concat([test3, test4], axis=1)
test6 = test5['Ratings'].apply(json.loads)


https://stackoverflow.com/questions/988228/convert-a-string-representation-of-a-dictionary-to-a-dictionary
https://stackoverflow.com/questions/20680272/parsing-a-json-string-which-was-loaded-from-a-csv-using-pandas
https://stackoverflow.com/questions/35297868/how-could-i-fix-the-unquoted-nan-value-in-json-using-python


test7=test6.reset_index(drop=True, inplace=True)

flatten_ratings = []
for item in range(0,5000):#len(test6)):
    if test6[item] is not None: #not pd.isnull(test6[item]):
        flatten_ratings.append(pd.DataFrame(test6[item]).set_index('Source').T)
        
flatten_ratings = pd.concat(flatten_ratings)  







