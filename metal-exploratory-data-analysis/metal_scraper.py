#! /usr/bin/env python
#
# Script for scraping album reviews and associated information from
# http://www.metal-archives.com
# BASED ON STUFF BY:
# Author: Jon Charest (http://github.com/jonchar)
# Year: 2016
#
# UPDATE THIS BLOCK IF EVER PUBLISH
# Approach:
# For each {nbr, A-Z}
# Read number of entries for given letter using result from `get_url`
# Determine how many requests of 200 entries are required, issue requests
# Read JSON in the `Requests` object returned by `get_url` using `r.json()`
# Read contents in 'aaData' key into a pandas `DataFrame`
# Set column names to `alpha_col_names`
# For each link in the list of reviews, visit link and extract the title
# of the review and the content of the review
# Save chunk of reviews to csv file

import time
import sys
from datetime import timedelta, datetime
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame
# from json.decoder import JSONDecodeError

testscrap = False

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
BASEURL = 'http://www.metal-archives.com'

# for genre to year
# RELURL_search =
# '/search/ajax-advanced/searching/albums?bandName=&releaseTitle=&releaseYearFrom=1900&releaseMonthFrom=01&releaseYearTo=*&releaseMonthTo=*&country=&location=&releaseLabelName=&releaseCatalogNumber=&releaseIdentifiers=&releaseRecordingInfo=&releaseDescription=&releaseNotes=&genre=*&releaseType[]=1&releaseFormat[]=CD&releaseFormat[]=Vinyl*#albums'

# for format to year
RELURL_search = '/search/ajax-advanced/searching/albums/?bandName=%2A&releaseTitle=%2A&releaseYearFrom=1900&releaseMonthFrom=&releaseYearTo=&releaseMonthTo=&country=&location=&releaseLabelName=&releaseCatalogNumber=&releaseIdentifiers=&releaseRecordingInfo=&releaseDescription=&releaseNotes=&genre=&releaseFormat[]=CD&releaseFormat[]=Cassette%2A&releaseFormat[]=Vinyl%2A&releaseFormat[]=VHS&releaseFormat[]=DVD&releaseFormat[]=Digital&releaseFormat[]=Blu-ray%2A&releaseFormat[]=Other'


response_len = 200  # NEEDS TO BE 200 for the ADVANCED SEARCH OUTPUT

encoding = 'UTF-8'


def get_url(letter='A', start=0, length=200):
    """Gets the review listings displayed as alphabetical tables on M-A for
    input `letter`, starting at `start` and ending at `start` + `length`.
    Returns a `Response` object. Data can be accessed by calling the `json()`
    method of the returned `Response` object."""

    payload = {'sEcho': 1,
               'iColumns': 7,
               'iDisplayStart': start,
               'iDisplayLength': length}

    r = requests.get(BASE_URL + URL_EXT_ALPHA + letter + URL_SUFFIX,
                     params=payload)

    return r


def get_search(start=0, length=200):

    payload = {'sEcho': 0,  # if not set, response text is not valid JSON
               'iDisplayStart': start,  # set start index of band names returned
               'iDisplayLength': length}  # only response lengths of 200 work

    r = requests.get(BASEURL + RELURL_search, params=payload, headers=headers)

    return r


print('starting the scraper')

# Data columns in the returned JSON for genre to year
# column_names = ['BandLink', 'AlbumLink',
# 'Genre', 'Date', 'CatalogNumber', 'Format']
# for format to year
column_names = ['BandLink', 'AlbumLink',
                'Type', 'Date', 'CatalogNumber', 'Format']


# date_col_names = ['Date', 'ReviewLink', 'BandLink', 'AlbumLink',
# 'Score', 'UserLink', 'Time']

# Valid letter entries for alphabetical listing
# letters = 'nbr A B C D E F G H I J K L M N O P Q R S T U V W X Y Z'.split()

# Valid date example for by-date listing (YYYY-MM)
date = '2016-04'

data = DataFrame()

date_of_scraping = datetime.utcnow().strftime('%Y-%m-%d')

print("start first call")
testResponse = get_search(0)

n_records = testResponse.json()['iTotalRecords']
n_chunks = int(n_records / response_len) + 1
print('Found ' + str(n_records) +
      ' Albums in selection, will be collected in ' + str(n_chunks) + ' chunks')

if(testscrap):
    n_chunks = 20
    print("get " + str(n_chunks) + " chunks for testing")

print("I am nice and wait 5 sec between each request")
time.sleep(5)

starttime = datetime.now()
for i in range(n_chunks):
    start = response_len * i
    if start + response_len < n_records:
        end = start + response_len
    else:
        end = n_records

    # sys.stdout.write('\r')
    # the exact output you're looking for:
    # sys.stdout.write("[%-20s] %d%%" % ('='*i, 5*i))
    estTime = str(timedelta(seconds=(
        (datetime.now() - starttime).total_seconds() * float(n_chunks) / float(i + 1))))
    # sys.stdout("Fetching album entries, %d to %d, %d perc done. Estimated time left %s" % (
    # int(start), int(end), int(float(i) / float(n_chunks) * 100.), estTime))
    # sys.stdout.flush()
    print('Fetching band entries ', str(start), 'to ',
          str(end), 'estimated time remaining: ', estTime)

    for attempt in range(10):
        time.sleep(5)
        try:
            r = get_search(start, response_len)
            js = r.json()
            # Store response
            df = DataFrame(js['aaData'])
            data = data.append(df)
            # If the response fails, r.json() will raise an exception, so retry
            # except JSONDecodeError:
        except:
            print('JSONDecodeError on attempt ', attempt, ' of 10.')
            print('Retrying...')
            continue
        break

# Set informative names
data.columns = column_names

# Current index corresponds to index in smaller chunks concatenated
# Reset index to start at 0 and end at number of bands
data.index = range(len(data))

# Save to CSV
f_name = 'MA-albums_{}_withFormat.csv'.format(date_of_scraping)
if(testscrap):
    f_name = "test_" + f_name
print('Writing band data to csv file:', f_name)
data.to_csv(f_name, encoding=encoding)
print('Complete!')
