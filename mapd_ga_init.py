#!/usr/bin/python
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
"""

__author__ = 'veda.shankar@gmail.com (Veda Shankar)'

import argparse
import sys
import string
import os

os.system("/usr/bin/clear")
print('Cut and Paste the contents of the client_secrets.json file below: ')
client_secrets = str(raw_input())
if client_secrets == '':
  print('Did not paste the contents of client_secrets.json, exiting ...')
  sys.exit(0)
print client_secrets
json_file = open('./client_secrets.json', 'w')
json_file.write(client_secrets)
json_file.close()

os.system("/usr/bin/clear")
os.system("python ./mapd_ga_data.py --noauth_local_webserver")

