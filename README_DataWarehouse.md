#Songify AWS Database Warehousing 
##The goal of this project is to take the song, artist and album information and transform it from json files to a Redshift cluster with organized tables with diststyle key logic. 

1. Start by Creating your AWS cluster with CreateCluster.ipynb
2. Run create_table.py in the TestingNotebook.ipynb to create the tables in Redshift
3. Run etl.py to copy log and event data into temporary table, and then into Redshift tables 


Important notes: 
- dwh.cfg has all the user_ids, host keys and configurations for the cluster
- Have an error? Run the stl_load_error query in the CreatCluster.ipynb to see what is wrong 
- CLOSE THE CLUSTER WHEN DONE! 
