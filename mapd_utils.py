#!/usr/bin/python
  
"""
  Module for connecting to MapD database and creating tables with the provided data.
"""

__author__ = 'veda.shankar@gmail.com (Veda Shankar)'

if __name__ == "__main__":
  import argparse
  import sys
  import string
  import csv

import os
import pandas as pd
from pymapd import connect

connection = "NONE"

# Connect to the DB
def connect_to_mapd(str_user, str_password, str_host, str_dbname):
  global connection
  connection = connect(user=str_user, password=str_password, host=str_host, dbname=str_dbname)
  print connection

def drop_table_mapd(table_name):
  command = 'drop table if exists %s' % (table_name)
  print command
  connection.execute(command)

def fix_date_table(csv_file):
  command = './fixdate.sh %s' % (csv_file)
  print command
  os.system(command)

def copy_file_for_upload(csv_file):
  command = './copy_file_for_upload.sh %s' % (csv_file)
  print command
  os.system(command)

# Load CSV to DB
def load_to_mapd(table_name, csv_file, field_name):
  global connection
  #csv_df = pd.read_csv(csv_file)
  #connection.create_table(table_name, csv_df, preserve_index=False)
  #connection.load_table(table_name, csv_df, preserve_index=False)
  print "loading ..."
  create_table_str = 'CREATE TABLE %s (ga_date DATE, ga_time TIME, ga_networkLocation TEXT ENCODING DICT(8), ga_longitude REAL, ga_latitude REAL, ga_landingPagePath TEXT ENCODING DICT(8), ga_%s TEXT ENCODING DICT(8), ga_pageviews BIGINT)' % (table_name, field_name)
  print create_table_str
  connection.execute(create_table_str)
  copy_file_for_upload(csv_file)
  query = 'COPY %s from \'/tmp/foobar_gauser.csv\' WITH (nulls = \'NA\')' % (table_name)
  print query
  connection.execute(query)
  connection.get_table_details(table_name)

