# import dependencies
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2
import time
from dateutil.relativedelta import relativedelta

# import db password
from config import db_password

# details of movie data files
file_dir = 'data/'

# generate regexs to match box office value formats
box_office_form_one = r'\$\s*\d+\.?\d*\s*[mb]illion'
box_office_form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illi?on)'

# regexs for date matching
date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
date_form_two = r'\d{4}.[01]\d.[123]\d'
date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
date_form_four = r'\d{4}'

# create connection string 
db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"

def movie_data_pipeline(wiki_data, kaggle_data, ratings_data):
	# create dataframes for data
	wiki_movies_df = pd.DataFrame(wiki_data)
	# make copies to prevent altering raw data
	kaggle_movies_df = kaggle_data.copy()
	ratings_df = ratings_data.copy()
	## Transform wiki Data
	# filter wiki movies to have director, imbd link, and no episodes
	wiki_movies = [movie for movie in wiki_data 
		if ('Director' in movie or 'Directed by' in movie) 
		and 'imdb_link' in movie and 'No. of episodes' not in movie]
	wiki_movies_df = pd.DataFrame(wiki_movies)

	# apply clean_movie function to wiki_movies_df
	clean_movies = [clean_movie(movie) for movie in wiki_movies]
	wiki_movies_df = pd.DataFrame(clean_movies)

	# drop duplicate cells based on imbd id
	wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
	wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

	# filter columns with >90% null values
	wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
	wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

	# save box office column without NAs
	box_office = extract_and_join_column('Box office', wiki_movies_df)
	# deal with rows that have a range
	box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

	# apply function to wiki_movies_df and drop original column
	wiki_movies_df['box_office'] = box_office.str.extract(f'({box_office_form_one}|{box_office_form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
	wiki_movies_df.drop('Box office', axis=1, inplace=True)

	# create a budget variable
	budget = extract_and_join_column('Budget', wiki_movies_df)
	# remove higher end of ranges (if present)
	budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
	# remove citation references
	budget = budget.str.replace(r'\[\d+\]\s*', '')

	# apply function to wiki_movies_df and drop original column
	wiki_movies_df['budget'] = budget.str.extract(f'({box_office_form_one}|{box_office_form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
	wiki_movies_df.drop('Budget', axis=1, inplace=True)

	# store non-null release dates
	release_date =extract_and_join_column('Release date', wiki_movies_df)

	# convert to datetime
	wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

	# store non null running times
	running_time = extract_and_join_column('Running time', wiki_movies_df)

	# extract times. 
	running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
	# convert strings to numerics
	running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

	# apply to DataFrame and drop original column
	wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
	wiki_movies_df.drop('Running time', axis=1, inplace=True)

	## Transform Kaggle Data
	# keep rows with adult == False
	kaggle_movies_df = kaggle_movies_df[kaggle_movies_df['adult'] == 'False'].drop('adult',axis='columns')

	# convert to Boolean dtype
	kaggle_movies_df['video'] = kaggle_movies_df['video'] == 'True'

	# attempt to convert numerics
	kaggle_movies_df['budget'] = kaggle_movies_df['budget'].astype(int)
	kaggle_movies_df['id'] = pd.to_numeric(kaggle_movies_df['id'], errors='raise')
	kaggle_movies_df['popularity'] = pd.to_numeric(kaggle_movies_df['popularity'], errors='raise')

	# convert to datetime dtype
	kaggle_movies_df['release_date'] = pd.to_datetime(kaggle_movies_df['release_date'])

	# inner merge wiki and kaggle data
	movies_df = pd.merge(wiki_movies_df, kaggle_movies_df, on='imdb_id', suffixes=['_wiki','_kaggle'])
	# can't easily check outliers, assume merge correctly

	# store languages as tuples for wiki column
	movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)

	# remove unwanted columns
	movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

	# fill in missing data from column pair then drops redundant column
	fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
	fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
	fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

	# reorder columns
	movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
    # rename columns
	movies_df.rename({'id':'kaggle_id',
	                  'title_kaggle':'title',
	                  'url':'wikipedia_url',
	                  'budget_kaggle':'budget',
	                  'release_date_kaggle':'release_date',
	                  'Country':'country',
	                  'Distributor':'distributor',
	                  'Producer(s)':'producers',
	                  'Director':'director',
	                  'Starring':'starring',
	                  'Cinematography':'cinematography',
	                  'Editor(s)':'editors',
	                  'Writer(s)':'writers',
	                  'Composer(s)':'composers',
	                  'Based on':'based_on'
	                 }, axis='columns', inplace=True)

	## Transform ratings_df Data
	# apply conversion to DataFrame
	ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'], unit='s')

	# get number of ratings
	rating_counts = ratings_df.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')

    # add qualifier to column names
	rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

	# left merge with movies_df
	movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

	# zero out missing values
	movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)


	## Load Data
	# create engine
	engine = create_engine(db_string)

	# save movie data to SQL table
	movies_df.to_sql(name='movies', con=engine, if_exists='append')
	print('Loaded movie data.\nBegining to load ratings data.')

	# import ratings data in chunks
	rows_imported = 0
	# get the start_time from time.time()
	start_time = time.time()
	for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
	    print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
	    data.to_sql(name='ratings', con=engine, if_exists='append')
	    rows_imported += len(data)

	    # add elapsed time to final print out
	    print(f'Done. {time.time() - start_time} total seconds elapsed')



def extract_raw_data():
	# load json data into list of dictionaries
	with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file:
	    wiki_movies_raw = json.load(file)
	# pull in Kaggle data
	kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
	ratings = pd.read_csv(f'{file_dir}ratings.csv')
	return wiki_movies_raw, kaggle_metadata, ratings


def extract_and_join_column(column_name, dataframe):
	try:
		non_null_column = dataframe[column_name].dropna()
		# convert any lists to strings
		result = non_null_column.map(lambda x: ' '.join(x) if type(x) == list else x)
	except KeyError:
		print(f'No column named {column_name} in {dataframe}')
		exit()

	return result


def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)


def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)
        # convert to float and multiply by a million
        value = float(s) * 10**6
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)
        # convert to float and multiply by a billion
        value = float(s) * 10**9
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
        # remove dollar sign and commas
        s = re.sub('\$|,','', s)
        # convert to float
        value = float(s)
        return value

    else:
        return np.nan



# aggregate columns and modify column names
def clean_movie(movie):
    # make a non destructive copy of the paramater
    movie = dict(movie)
    # make an empty dict to hold all alternative titles
    alt_titles = {}
    
    # loop through list of alternative title keys
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    # add alternative titles dict to movie object
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles
        
    # merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')
        
    return movie


if __name__ == "__main__":
	wiki, kaggle, rating = extract_raw_data()
	movie_data_pipeline(wiki, kaggle, rating)





