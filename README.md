# Exploring Google Analytics Data with MapD
Explore Google Analytics data with a platform like MapD to extract insights that are hard to find using Google's standard interface. Download the different metrics from Google Analytics account in CSV format and upload the data into MapD Core database. You can then use MapD Immerse platform to explore the GA metrics visually.

- Install Software Packages
# pip install --upgrade google-api-python-client
# pip install matplotlib pandas python-gflags
# pip install pymapd
# git clone https://github.com/omveda/google-analytics.git (this repo)

- Usage
Copy the client_secrets.json corresponding to the Google Analytics Service Account to the directory you cloned this repository.
Run the command:
- python mapd_ga_data.py

- Workflow of the MapD-Google Analytics Application
This application is written in Python and based on the original work from Ryan Praski http://www.ryanpraski.com/python-google-analytics-api-unsampled-data-multiple-profiles/ and Google Developer’s Hello Analytics API: Python quickstart for service accounts sample program.  
Here is what the application does:
Create a service object to Google Analytics using the client_secrets.json file corresponding to the service account that was created in the prerequisite step. Traverse the Google Analytics management hierarchy and construct the mapping of website profile views and IDs. Prompt the user to select the profile view they would like to collect the data.
 
$ python mapd_ga_data.py 
Item#           Profile ID               Profile Name
   1            151405570       MapD Community edition AWS_All Web Site Data
   2            151400370       MapD Community edition DL_All Web Site Data
   3            160834939       MapD Community edition DL_Non-Dev
   4            161106277       MapD Community edition DL_Non-MapD
   5            152688532       MapD Enterprise AWS_All Web Site Data
   6            108540717       MapD Enterprise installed_All Web Site Data
   7             94937405       MapD MIT Twitter Demo_All Web Site Data
   8            134050322       MapD Meetup - Enterprise GPU Computing_All Web 
   9            152769575       MapD OS_All Web Site Data
  10            168795059       MapD Test Drive_All Web Site Data
  11            122232033       MapD Testing_All Web Site Data
  12             93521025       MapD Website_Live Site
  13            123255922       MapD Website_No Filters
  14            160571176       MapD Website_Staging
  15            160593821       MapD Website_Staging (testing)
  16             94956153       MapD YouTube_All Web Site Data
Enter the item# of the profile you would like to upload:  2
Item # 2 selected

Prompt the user to enter the begin and end date range for downloading the data. If you just hit enter without any dates, the application will by default fetch the last 30 days worth of data.
 
Enter the begin date and end date in the following format: YYYY-MM-DD YYYY-MM-DD
Or hit enter to proceed with the default which is last 30 days data
Date Range:  2017-08-01 2018-02-20
Extract data from 2017-08-01 to 2018-02-20

Prompt the user to enter the MapD server specific information which include the name of the server, database login, database password, database name and server ssh login name. The ssh login name will be used to copy the CSV file to a temporary location on the MapD server prior to loading it into the database. If you don’t want to automatically upload the analytics data to MapD and would like to upload it using the Immerse UI, simply hit enter without any information.
 
Enter the MapD server information if you want to upload data,
 otherwise simply hit enter to use the manual procedure to upload the data
  Information needed - <Hostname or IP Address> <db login> <db password> <database name> <SSH login>
MapD Server Info:  mapd-my-azure-server mapd HyperInteractive mapd john
The output CSV file will be automatically uploaded to mapd-my-azure-server server into mapd database
 
Going to download data for MapD Community edition DL_All Web Site Data(151400370) ...
ga:date,ga:hour,ga:minute,ga:longitude,ga:latitude,ga:landingPagePath,ga:networkLocation
Found 48560 number of records
Profile Name: All Web Site Data
Now pulling data from 2017-08-01 to 2018-02-20.
  < … >

The application then starts downloading the records for the selected profile view and saves it to the data directory under the names with the following pattern <profile name>_<dimension>.csv.  After all the records are saved to the CSV files the function merge_tables() is called to merge all the dimensions from the various tables into a single CSV file <profile name>.csv.  The merge function eliminates records that have empty location (latitude, longitude) fields.  The date and time dimensions are combined into a single DATETIME column which is compatible with MapD DDL.  If the user entered the MapD credentials while launching the application then the program will invoke the functions in mapd_utils.py file to load the table into MapD.

Connect to MapD Server
# Connect to the DB
def connect_to_mapd(str_user, str_password, str_host, str_dbname):
  global connection
  connection = connect(user=str_user, password=str_password, host=str_host, dbname=str_dbname)
  print connection

Drop Table if it exists
def drop_table_mapd(table_name):
  command = 'drop table if exists %s' % (table_name)
  print command
  connection.execute(command)


Create table and load data from the CSV file
# Load CSV to Table
def load_to_mapd(table_name, csv_file, mapd_host, mapd_user):
  global connection
  create_table_str = 'CREATE TABLE %s (ga_date TIMESTAMP, ga_longitude FLOAT, ga_latitude FLOAT, ga_landingPagePath TEXT ENCODING DICT(8), ga_networkLocation TEXT ENCODING DICT(8), ga_pageviews BIGINT, ga_country TEXT ENCODING DICT(8), ga_city TEXT ENCODING DICT(8), ga_source TEXT ENCODING DICT(8), ga_sessionDurationBucket BIGINT, ga_sessionCount BIGINT, ga_deviceCategory TEXT ENCODING DICT(8), ga_campaign TEXT ENCODING DICT(8), ga_adContent TEXT ENCODING DICT(8), ga_keyword TEXT ENCODING DICT(8))' % (table_name)
  print create_table_str
  connection.execute(create_table_str)
  server_csv_file = '/tmp/%s' % (os.path.basename(csv_file))
  command = 'scp %s %s@%s:%s' % (csv_file, mapd_user, mapd_host, server_csv_file)
  print command
  os.system(command)
 
  query = 'COPY %s from \'%s\' WITH (nulls = \'None\')' % (table_name, server_csv_file)
  print query
  connection.execute(query)
  print connection.get_table_details(table_name)
MapD Immerse Import Wizard
In case you want to manually load the table into MapD Core Database then you can use MapD Immerse’s table import feature.  Open MapD Immerse -> Click Data Manager -> Click Import Data -> Click the + sign or drag-and-drop the CSV file for upload.  Detailed documentation.

