#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""The following application is based on the work from:
   http://www.ryanpraski.com/python-google-analytics-api-unsampled-data-multiple-profiles/

   This application demonstrates how to use the python client library to access
   Google Analytics data and load it to MapD's database (www.mapd.com).
"""

"""
This application demonstrates how to use the python client library to access
Google Analytics data. The sample traverses the Management API to obtain the
authorized user's first profile ID. Then the sample uses this ID to
contstruct a Core Reporting API query to return the top 25 organic search
terms.

Before you begin, you must sigup for a new project in the Google APIs console:
https://code.google.com/apis/console

Then register the project to use OAuth2.0 for installed applications.

Finally you will need to add the client id, client secret, and redirect URL
into the client_secrets.json file that is in the same directory as this sample.

Sample Usage:

  $ python hello_analytics_api_v3.py

Also you can also get help on all the command-line flags the program
understands by running:

  $ python hello_analytics_api_v3.py --help
"""

__author__ = 'veda.shankar@gmail.com (Veda Shankar)'

import argparse
import sys
import csv
import string
import os
import re
# MAPD Modules
from mapd_utils import *

from apiclient.errors import HttpError
from apiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError

class SampledDataError(Exception): pass

# original set - key_dimensions=['ga:date','ga:hour','ga:minute','ga:networkLocation','ga:browserSize','ga:browserVersion']
#original set - all_dimensions=['ga:userAgeBracket','ga:userGender','ga:country','ga:countryIsoCode','ga:city','ga:continent','ga:subContinent','ga:userType','ga:sessionCount','ga:daysSinceLastSession','ga:sessionDurationBucket','ga:referralPath','ga:browser','ga:operatingSystem','ga:browserSize','ga:screenResolution','ga:screenColors','ga:flashVersion','ga:javaEnabled','ga:networkLocation','ga:mobileDeviceInfo','ga:mobileDeviceModel','ga:mobileDeviceBranding','ga:deviceCategory','ga:language','ga:adGroup','ga:source','ga:dataSource','ga:sourceMedium','ga:adSlot','ga:mobileInputSelector','ga:mobileDeviceMarketingName','ga:searchCategory','ga:searchDestinationPage','ga:interestAffinityCategory','ga:landingPagePath','ga:exitPagePath','ga:browserVersion','ga:eventLabel','ga:eventAction','ga:eventCategory','ga:hour','ga:yearMonth','ga:Month','ga:date','ga:keyword','ga:campaign','ga:adContent']

key_dimensions=['ga:date','ga:hour','ga:networkLocation','ga:longitude','ga:latitude','ga:landingPagePath']
all_dimensions=['ga:source','ga:mobileDeviceModel','ga:mobileDeviceBranding','ga:mobileDeviceInfo','ga:daysSinceLastSession','ga:sessionCount','ga:countryIsoCode','ga:javaEnabled','ga:sessionDurationBucket','ga:language','ga:screenColors','ga:operatingSystem','ga:deviceCategory','ga:browserSize','ga:flashVersion','ga:continent','ga:screenResolution','ga:browser', 'ga:adGroup', 'ga:adContent', 'ga:adSlot', 'ga:keyword','ga:campaign', 'ga:city', 'ga:country']
n_dims = 7 - len(key_dimensions)

# Traverse the GA management hierarchy and construct the mapping of website 
# profile views and IDs.
def traverse_hierarchy(argv):
  # Authenticate and construct service.
  service, flags = sample_tools.init(
      argv, 'analytics', 'v3', __doc__, __file__,
      scope='https://www.googleapis.com/auth/analytics.readonly')

  try:
    accounts = service.management().accounts().list().execute()
    for account in accounts.get('items', []):
      accountId = account.get('id')
      webproperties = service.management().webproperties().list(
          accountId=accountId).execute()
      for webproperty in webproperties.get('items', []):
        firstWebpropertyId = webproperty.get('id')
        profiles = service.management().profiles().list(
            accountId=accountId,
            webPropertyId=firstWebpropertyId).execute()
        for profile in profiles.get('items', []):
          profileID = "%s" % (profile.get('id'))
          profileName = "%s" % (profile.get('name'))
          profileName = profileName.strip()
          profileName = profileName.replace(' ','')
          profile_ids[profileName] = profileID

  except TypeError as error:
    # Handle errors in constructing a query.
    print(('There was an error in constructing your query : %s' % error))

  except HttpError as error:
    # Handle API errors.
    print(('Arg, there was an API error : %s : %s' %
           (error.resp.status, error._get_reason())))

  except AccessTokenRefreshError:
    print ('The credentials have been revoked or expired, please re-run'
           'the application to re-authorize')

def main(argv):
  for dim_i in xrange(0,len(all_dimensions), n_dims):
    dimss = key_dimensions + all_dimensions[dim_i:dim_i+n_dims]
    dims = ",".join(dimss)
    #path = os.path.abspath('.') + '/data/'
    path = './data/'
    if not os.path.exists(path):
      os.mkdir(path, 0755)
    file_suffix = '%s' % (all_dimensions[dim_i:dim_i+n_dims])
    file_suffix = '%s' % (file_suffix.strip('\[\'\]'))
    file_suffix = '%s' % (file_suffix[3:])
    filename = '%s_%s.csv' % (profile.lower(), file_suffix)
    table_names[dims] = '%s_%s' % (profile.lower(), file_suffix)
    table_filenames[dims] = path + '%s_%s.csv' % (profile.lower(), file_suffix)
#RM filename = 'ga_%s_%s.csv' % (profile.lower(), dims)
    files[dims] = open(path + filename, 'wt')
    writers[dims] = csv.writer(files[dims], lineterminator='\n')

  # Authenticate and construct service.
  service, flags = sample_tools.init(
      argv, 'analytics', 'v3', __doc__, __file__,
      scope='https://www.googleapis.com/auth/analytics.readonly')

  # Try to make a request to the API. Print the results or handle errors.
  try:
    profile_id = profile_ids[profile]
    if not profile_id:
      print 'Could not find a valid profile for this user.'
    else:
      for start_date, end_date in date_ranges:
        for dim_i in xrange(0,len(all_dimensions), n_dims):
          dimss = key_dimensions + all_dimensions[dim_i:dim_i+n_dims]
          dims = ",".join(dimss)
          print dims
          limit = ga_query(service, profile_id, 0,
                                   start_date, end_date, dims).get('totalResults')
          print "Found " + str(limit) + " number of records" #VS
          #for pag_index in xrange(0, limit, 10000):  #Do 10K records for testing
          #for pag_index in xrange(0, 10000, 10000): #VS
          for pag_index in xrange(0, limit, 10000):  #Do 10K records for testing
            results = ga_query(service, profile_id, pag_index,
                                       start_date, end_date, dims)
            if results.get('containsSampledData'):
              raise SampledDataError #VS
            print_results(results, pag_index, start_date, end_date, dims)

  except TypeError, error:
    # Handle errors in constructing a query.
    print ('There was an error in constructing your query : %s' % error)

  except HttpError, error:
    # Handle API errors.
    print ('Arg, there was an API error : %s : %s' %
           (error.resp.status, error._get_reason()))

  except AccessTokenRefreshError:
    # Handle Auth errors.
    print ('The credentials have been revoked or expired, please re-run '
           'the application to re-authorize')

  except SampledDataError:
    # force an error if ever a query returns data that is sampled!
    print ('Error: Query contains sampled data!')


def ga_query(service, profile_id, pag_index, start_date, end_date, dims):

  return service.data().ga().get(
      ids='ga:' + profile_id,
      start_date=start_date,
      end_date=end_date,
      metrics='ga:pageviews',
      dimensions=dims,
      sort='-ga:pageviews',
      samplingLevel='HIGHER_PRECISION',
      start_index=str(pag_index+1),
      max_results=str(pag_index+10000)).execute()


def print_results(results, pag_index, start_date, end_date, dims):
  """Prints out the results.

  This prints out the profile name, the column headers, and all the rows of
  data.

  Args:
    results: The response returned from the Core Reporting API.
  """

  # New write header
  if pag_index == 0:
    if (start_date, end_date) == date_ranges[0]:
      print 'Profile Name: %s' % results.get('profileInfo').get('profileName')
      columnHeaders = results.get('columnHeaders')
      #cleanHeaders = [str(h['name']) for h in columnHeaders]
      #writers[dims].writerow(cleanHeaders)
      cleanHeaders_str = '%s' % [str(h['name']) for h in columnHeaders]
      cleanHeaders_str = cleanHeaders_str.replace(':', '_')
      cleanHeaders_str = cleanHeaders_str.replace('\'', '')
      cleanHeaders_str = cleanHeaders_str.replace('[', '')
      cleanHeaders_str = cleanHeaders_str.replace(']', '')
      cleanHeaders_str = cleanHeaders_str.replace(',', '')
      cleanHeaders = cleanHeaders_str.split()
      writers[dims].writerow(cleanHeaders)
    print 'Now pulling data from %s to %s.' %(start_date, end_date)

  # Print data table.
  if results.get('rows', []):
    for row in results.get('rows'):
      for i in range(len(row)):
        old, new = row[i], str()
        for s in old:
          new += s if s in string.printable else ''
        row[i] = new
      writers[dims].writerow(row)

  else:
    print 'No Rows Found'

  limit = results.get('totalResults')
  print pag_index, 'of about', int(round(limit, -4)), 'rows.'
  return None

profile_ids = {}

date_ranges = [('2010-01-01',
               '2018-01-31')]

writers = {}
files = {}
table_names = {}
table_filenames = {}

# Construct dictionary of GA website name and ids.
traverse_hierarchy(sys.argv)

# Select the GA profile view to extract data
selection_list = [0]
i = 1
print ("Item#  ProfileName  ProfileID")
for profile in sorted(profile_ids):
  selection_list = selection_list + [profile_ids[profile]]
  print ('%s) %s %s' % (i, profile, profile_ids[profile]))
  i +=1

print('Enter the item# of the profile you would like to upload: ')
item = int(raw_input())
if item == '' or item <= 0 or item >= len(selection_list):
  print('Invalid selection - %s' % item)
  sys.exit(0)
print('item # %s selected' % item)
for profile in sorted(profile_ids):
  if (selection_list[item] == profile_ids[profile]):
    print ('going to load %s(%s) ...' % (profile, profile_ids[profile]))
    main(sys.argv)
print "Profile done. Load to MapD ..."

# Connect to MapD
connect_to_mapd("gauser01", "GoogleAnalytics1@", "mapd-azure-server", "gauser01db")
#VS: Error handling

# Load to MapD
for dim_i in xrange(0,len(all_dimensions), n_dims):
  dimss = key_dimensions + all_dimensions[dim_i:dim_i+n_dims]
  dims = ",".join(dimss)
  field_name = '%s' % (all_dimensions[dim_i:dim_i+n_dims])
  field_name = filter(str.isalnum, field_name)
  print ('Loading data to MapD from file %s ...' % (table_filenames[dims]))
  files[dims].close()
  fix_date_table(table_filenames[dims])
  drop_table_mapd(table_names[dims])
  load_to_mapd(table_names[dims], table_filenames[dims], field_name[2:])

print "======================================================================="
print "Goto MapD Immerse UI @ http://community-azure.mapd.com:9092/"
print "======================================================================="

