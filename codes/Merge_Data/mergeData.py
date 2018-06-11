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

movies_full_test2=movies_full_test.drop(columns=['belongs_to_collection', 'cast_x', 'cast_y','genres', 'genres_x', \
                                'genres_y', 'keywords_x', 'keywords_y','production_companies','production_companies_x', \
                                'production_companies_y','Ratings'])

# =============================================================================
# Rename same-named columns
# 
# =============================================================================
column_names = movies_full_test2.columns.values
column_names[34] = 'title_1'
column_names[67] = 'title_2'
movies_full_test2.columns = column_names

# =============================================================================
# Remove similar/duplicate movie title columns
# 
# =============================================================================
movie_name_list_columns = ['Movie', 'original_title','original_title_x','original_title_y','Title','title_x', \
                   'title_x', 'title_y', 'title_y']

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

movies_full_test2['BoxOfficeFixed'] = movies_full_test2['BoxOffice']
movies_full_test2['BoxOfficeFixed'] = movies_full_test2['BoxOfficeFixed'].apply(lambda x: \
                 float(x.strip('$').replace(',','')) if  type(x)==str and 'k' not in x else np.nan) 


movie_rev_list_columns = ['BoxOfficeFixed','revenue','revenue_x','revenue_y','Worldwide Gross']

def getMovieRev(x, columns):
    return x[['BoxOfficeFixed','revenue','revenue_x','revenue_y','Worldwide Gross']].max()


movies_full_test2['Movie_Revenue'] = movies_full_test2.apply(getMovieRev, args=(movie_rev_list_columns,), axis=1)

movies_full_test2=movies_full_test2.drop(columns=[movie_rev_list_columns, 'Domestic Gross', 'BoxOffice'])