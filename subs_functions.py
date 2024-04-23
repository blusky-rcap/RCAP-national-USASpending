import os
import csv
import json
import time
import re
import zipfile
import pprint
import dask.dataframe as dd
import numpy as np
import pandas as pd
import glob
from datetime import datetime


# wrote this yrs ago and don't remember why I did it this way
def read_csv(file):
    arr = []
    with open(file, newline='', encoding="ISO-8859-1") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            try:
                arr.append(row)
            except UnicodeDecodeError:
                print("decode error reading CSV: ", row)
                continue
    csvfile.close()
    return arr


# typing func
def convert_float_from_str(val):
    n = 0.00
    try:
        n = np.float64(val)
        return n
    except ValueError:
        n = 0.00
        return n


# typing func
def convert_int_from_str(val):
    try:
        output = int(val)
        return output
    except ValueError:
        return 0
    except TypeError:
        return 0


def total_per_cfda_by_city(row, city):
    print('h')
    print(row.head())





# indices of all the columns we care about
cols_in = [
    2,
    41,
    46,
    65,
    66,
    71,
    76,
    84,
    86,
    92,
    94,
    100,
]

dtypes_in = {
    'prime_award_amount': convert_float_from_str,  # 2
    'prime_awardee_city_name': str,  # 41
    'prime_awardee_state_name': str,  # 46
    'prime_award_cfda_numbers_and_titles': str,  # 65
    'subaward_type': str,  # 66
    'subaward_amount': convert_float_from_str,  # 71
    'subawardee_name': str,  # 76
    'subawardee_city_name': str,  # 84
    'subawardee_state_name': str,  # 86
    'subaward_primary_place_of_performance_city_name': str,  # 92
    'subaward_primary_place_of_performance_state_name': str,  # 94
    'subaward_description': str,  # 100


}


SUBAWARD_CFDAs = [
 14.218,
 14.228,
 14.248,
 14.269,
 14.862,
 66.458,  # per victorias req
 66.468,
 14.225,
 66.448,
 11.3
]

SUBAWARD_cdbg_str = [  # need keyword filtering
 '14.218',
 '14.225',
 '14.228',
 '14.248',
 '14.269',
 '14.862',
]

SUBAWARD_srf_str = [  # dont need keyword filtering
 '66.458',
 '66.468',
 '66.482',
 '66.483'
]

SUBAWARD_str = [
 '14.218',
 '14.225',
 '14.228',
 '14.248',
 '14.269',
 '14.862',
 '66.458',
 '66.468',
 '66.482',
 '66.483'
]

water_keywords = [
    'water',
    'pipe',
    'drain',
    'wastewater',
    'sewer',
    'sewage',
    'drinking',
    'sink',
    'pump',
    'storm',
    'filtration',
    'faucet',
    'fountain',
    'sump',
    'stormwater',
    'treatment plant',
    'hydrant',
    'plumbing',
    'sanitary'
]
# List of states
state_abbrev = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'IA': 'Iowa',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'MA': 'Massachusetts',
    'MD': 'Maryland',
    'ME': 'Maine',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MO': 'Missouri',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'NE': 'Nebraska',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NV': 'Nevada',
    'NY': 'New York',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming',
    'PR': 'Puerto Rico',
    'VI': 'Virgin Islands'
}

states = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AS': 'American Samoa',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'GU': 'Guam',
    'HI': 'Hawaii',
    'IA': 'Iowa',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'MA': 'Massachusetts',
    'MD': 'Maryland',
    'ME': 'Maine',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MO': 'Missouri',
    'MP': 'Northern Mariana Islands',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NA': 'National',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'NE': 'Nebraska',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NV': 'Nevada',
    'NY': 'New York',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'PR': 'Puerto Rico',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VI': 'Virgin Islands',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming'
}


def best_match(x):
    try:
        if len(x) == 2:  # Try another way for 2-letter codes
            for a, n in states.items():
                if len(n.split()) == 2:
                    if "".join([c[0] for c in n.split()]).lower() == x.lower():
                        return n.upper()
        new_rx = re.compile(r"\w*".join([ch for ch in x]), re.I)
        for a, n in states.items():
            if new_rx.match(n):
                return n.upper()
    except TypeError:
        print(x)
        return 'ERROR'


