Purpose: Take song data from AWS S3, transform it into tables with spark processing and parquet back into an S3 bucket. 

dl.cfg has the AWS credentials 
etl.py has the song and log json file processing information 
RunNotebook.ipynb is what you can use to run the etl. 

This script is successful if you see the artist, song, user, time and songplay parquet in the S3 blucket. 

