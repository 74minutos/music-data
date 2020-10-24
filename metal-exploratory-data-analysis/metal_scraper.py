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

# for genre to year (with country)
# RELURL_search =
 '/search/ajax-advanced/searching/albums?bandName=&releaseTitle=&releaseYearFrom=1900&releaseMonthFrom=01&releaseYearTo=2020&releaseMonthTo=12&country%5B%5D=AF&country%5B%5D=AX&country%5B%5D=AL&country%5B%5D=DZ&country%5B%5D=AS&country%5B%5D=AD&country%5B%5D=AO&country%5B%5D=AI&country%5B%5D=AQ&country%5B%5D=AG&country%5B%5D=AR&country%5B%5D=AM&country%5B%5D=AW&country%5B%5D=AU&country%5B%5D=AT&country%5B%5D=AZ&country%5B%5D=BS&country%5B%5D=BH&country%5B%5D=BD&country%5B%5D=BB&country%5B%5D=BY&country%5B%5D=BE&country%5B%5D=BZ&country%5B%5D=BJ&country%5B%5D=BM&country%5B%5D=BT&country%5B%5D=BO&country%5B%5D=BQ&country%5B%5D=BA&country%5B%5D=BW&country%5B%5D=BV&country%5B%5D=BR&country%5B%5D=IO&country%5B%5D=BN&country%5B%5D=BG&country%5B%5D=BF&country%5B%5D=BI&country%5B%5D=KH&country%5B%5D=CM&country%5B%5D=CA&country%5B%5D=CV&country%5B%5D=KY&country%5B%5D=CF&country%5B%5D=TD&country%5B%5D=CL&country%5B%5D=CN&country%5B%5D=CX&country%5B%5D=CC&country%5B%5D=CO&country%5B%5D=KM&country%5B%5D=CD&country%5B%5D=CG&country%5B%5D=CK&country%5B%5D=CR&country%5B%5D=HR&country%5B%5D=CU&country%5B%5D=CW&country%5B%5D=CY&country%5B%5D=CZ&country%5B%5D=DK&country%5B%5D=DJ&country%5B%5D=DM&country%5B%5D=DO&country%5B%5D=TL&country%5B%5D=EC&country%5B%5D=EG&country%5B%5D=SV&country%5B%5D=GQ&country%5B%5D=ER&country%5B%5D=EE&country%5B%5D=SZ&country%5B%5D=ET&country%5B%5D=FK&country%5B%5D=FO&country%5B%5D=FJ&country%5B%5D=FI&country%5B%5D=FR&country%5B%5D=GF&country%5B%5D=PF&country%5B%5D=TF&country%5B%5D=GA&country%5B%5D=GM&country%5B%5D=GE&country%5B%5D=DE&country%5B%5D=GH&country%5B%5D=GI&country%5B%5D=GR&country%5B%5D=GL&country%5B%5D=GD&country%5B%5D=GP&country%5B%5D=GU&country%5B%5D=GT&country%5B%5D=GG&country%5B%5D=GN&country%5B%5D=GW&country%5B%5D=GY&country%5B%5D=HT&country%5B%5D=HM&country%5B%5D=HN&country%5B%5D=HK&country%5B%5D=HU&country%5B%5D=IS&country%5B%5D=IN&country%5B%5D=ID&country%5B%5D=XX&country%5B%5D=IR&country%5B%5D=IQ&country%5B%5D=IE&country%5B%5D=IM&country%5B%5D=IL&country%5B%5D=IT&country%5B%5D=CI&country%5B%5D=JM&country%5B%5D=JP&country%5B%5D=JE&country%5B%5D=JO&country%5B%5D=KZ&country%5B%5D=KE&country%5B%5D=KI&country%5B%5D=KP&country%5B%5D=KR&country%5B%5D=KW&country%5B%5D=KG&country%5B%5D=LA&country%5B%5D=LV&country%5B%5D=LB&country%5B%5D=LS&country%5B%5D=LR&country%5B%5D=LY&country%5B%5D=LI&country%5B%5D=LT&country%5B%5D=LU&country%5B%5D=MO&country%5B%5D=MG&country%5B%5D=MW&country%5B%5D=MY&country%5B%5D=MV&country%5B%5D=ML&country%5B%5D=MT&country%5B%5D=MH&country%5B%5D=MQ&country%5B%5D=MR&country%5B%5D=MU&country%5B%5D=YT&country%5B%5D=MX&country%5B%5D=FM&country%5B%5D=MD&country%5B%5D=MC&country%5B%5D=MN&country%5B%5D=ME&country%5B%5D=MS&country%5B%5D=MA&country%5B%5D=MZ&country%5B%5D=MM&country%5B%5D=NA&country%5B%5D=NR&country%5B%5D=NP&country%5B%5D=NL&country%5B%5D=NC&country%5B%5D=NZ&country%5B%5D=NI&country%5B%5D=NE&country%5B%5D=NG&country%5B%5D=NU&country%5B%5D=NF&country%5B%5D=MK&country%5B%5D=MP&country%5B%5D=NO&country%5B%5D=OM&country%5B%5D=PK&country%5B%5D=PW&country%5B%5D=PS&country%5B%5D=PA&country%5B%5D=PG&country%5B%5D=PY&country%5B%5D=PE&country%5B%5D=PH&country%5B%5D=PN&country%5B%5D=PL&country%5B%5D=PT&country%5B%5D=PR&country%5B%5D=QA&country%5B%5D=RE&country%5B%5D=RO&country%5B%5D=RU&country%5B%5D=RW&country%5B%5D=BL&country%5B%5D=KN&country%5B%5D=LC&country%5B%5D=MF&country%5B%5D=PM&country%5B%5D=VC&country%5B%5D=WS&country%5B%5D=SM&country%5B%5D=ST&country%5B%5D=SA&country%5B%5D=SN&country%5B%5D=RS&country%5B%5D=SC&country%5B%5D=SL&country%5B%5D=SG&country%5B%5D=SX&country%5B%5D=SK&country%5B%5D=SI&country%5B%5D=SB&country%5B%5D=SO&country%5B%5D=ZA&country%5B%5D=GS&country%5B%5D=SS&country%5B%5D=ES&country%5B%5D=LK&country%5B%5D=SH&country%5B%5D=SD&country%5B%5D=SR&country%5B%5D=SJ&country%5B%5D=SE&country%5B%5D=CH&country%5B%5D=SY&country%5B%5D=TW&country%5B%5D=TJ&country%5B%5D=TZ&country%5B%5D=TH&country%5B%5D=TG&country%5B%5D=TK&country%5B%5D=TO&country%5B%5D=TT&country%5B%5D=TN&country%5B%5D=TR&country%5B%5D=TM&country%5B%5D=TC&country%5B%5D=TV&country%5B%5D=UM&country%5B%5D=UG&country%5B%5D=UA&country%5B%5D=AE&country%5B%5D=GB&country%5B%5D=US&country%5B%5D=ZZ&country%5B%5D=UY&country%5B%5D=UZ&country%5B%5D=VU&country%5B%5D=VA&country%5B%5D=VE&country%5B%5D=VN&country%5B%5D=VG&country%5B%5D=VI&country%5B%5D=WF&country%5B%5D=EH&country%5B%5D=YE&country%5B%5D=ZM&country%5B%5D=ZW&location=&releaseLabelName=&releaseCatalogNumber=&releaseIdentifiers=&releaseRecordingInfo=&releaseDescription=&releaseNotes=&genre=*&releaseType%5B%5D=1&releaseType%5B%5D=3&releaseType%5B%5D=5#albums'

# for format to year
# RELURL_search = '/search/ajax-advanced/searching/albums/?bandName=%2A&releaseTitle=%2A&releaseYearFrom=1900&releaseMonthFrom=&releaseYearTo=&releaseMonthTo=&country=&location=&releaseLabelName=&releaseCatalogNumber=&releaseIdentifiers=&releaseRecordingInfo=&releaseDescription=&releaseNotes=&genre=&releaseFormat[]=CD&releaseFormat[]=Cassette%2A&releaseFormat[]=Vinyl%2A&releaseFormat[]=VHS&releaseFormat[]=DVD&releaseFormat[]=Digital&releaseFormat[]=Blu-ray%2A&releaseFormat[]=Other'


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
                'Type', 'Genre', 'Country', 'Date']


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
