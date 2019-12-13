# Movies ETL

## Overview
Amazing Prime video, an online streaming service, has decided to sponser a Hackathon. Given a set of clean movie data, the task is to predict the popular films. The first step is to collect the data by extracting it from the web and transforming it and then transform the data to be more usable. Second, the data needs to be loaded into a SQL Database.

## Resources
Data Sources:
- movies_metadata.csv
- ratings.csv
- wikipedia-movies.json

Software:
- Anacoda 4.7.12
- Psycopg2 2.8.4

## Summary
The ETL (Extract, Transform, Load) process is an essential pipeline for data analysis. In order to analyze data, it must be collected, cleaned, and structured. This translates to extracting raw data from sources, cleaning and structuring the data into a desired form, and finally storing the data in a database. The challenge comes when combining data from multiple sources and the involved process of cleaning the data. When it comes to raw data, it is typically messy, meaning there could be NaN's, incorrect, or incomplete data. Then comes the transform step which deals with this messy data with the goal of structuring it in a way that can be easily written to a database. The extract and transform steps can be iterative meaning there may be multiple rounds of data collection and manipulation. This module dealt with three data sources and involved extracting the raw data from these files, individually cleaning them, then transforming them into a single structure which is then stored as a MySQL database.

## Challenge Overview
Amazing Prime video wants to be able to have an autonomous ETL pipeline. Expecting the same data formats, design a function that will perform the ETL process on the data.

## Challenge Summary
In order to automate the ETL pipeline, there are some necessary assumptions to be made. These assumptions come into play during the transform phase when cleaning the data. The first assumption is that movies will have no episodes listed. Another is that when movies have a price range for their profits, the lower end of the range is what's significant. In terms of row-completeness, mostly null columns are worth dropping. Non-adult films are selected for as well. Finally, a dangerous assumption is made in that merges go unchecked and are expected to complete without error. When creating an automated process, it is necessary to make assumptions to simplify the tasks.