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


# this function handles state name error checking
def best_match(x):
    x = str(x)
    if len(x) == 2:  # Try another way for 2-letter codes
        for a, n in states.items():
            if len(n.split()) == 2:
                if "".join([c[0] for c in n.split()]).lower() == x.lower():
                    return n.upper()
    new_rx = re.compile(r"\w*".join([ch for ch in x]), re.I)
    for a, n in states.items():
        if new_rx.match(n):
            return n.upper()

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


# indices of all the columns we care about
cols_in = [0, 6, 7, 8, 10, 11, 13, 14, 15, 21, 22, 43, 51, 52, 53, 54, 55, 56, 59, 60, 67, 70, 71, 72, 73, 75, 77, 78,
           80]

# data types/typing funcs for each column of incoming data
dtypes_in = {
    'assistance_transaction_unique_key': str,  # 0
    'federal_action_obligation': convert_float_from_str,  # 6
    'total_obligated_amount': convert_float_from_str,  # 7
    'non_federal_funding_amount': convert_float_from_str,  # 10
    'total_non_federal_funding_amount': convert_float_from_str,  # 11
    # 'face_value_of_loan': convert_float_from_str,  # 12 - column didnt show up anywhere
    'original_loan_subsidy_cost': convert_float_from_str,  # 13
    'total_face_value_of_loan': convert_float_from_str,  # 14
    'total_loan_subsidy_cost': convert_float_from_str,  # 15
    'total_outlayed_amount_for_overall_award': convert_float_from_str,  # 8
    'action_date': str,  # 21
    # 'recipient_uei': str,
    # 'recipient_duns': str,
    'recipient_name': str,  # 43
    'action_date_fiscal_year': convert_int_from_str,  # 22
    'recipient_county_code': convert_int_from_str,  # 49
    'recipient_address_line_1': str,  # 51
    'recipient_address_line_2': str,  # 52
    'recipient_city_code': str,  # 53
    'recipient_city_name': str,  # 54
    'recipient_county_name': str,  # 56
    'recipient_state_code': str,  # 58
    'recipient_zip_code': convert_int_from_str,  # 60
    'primary_place_of_performance_scope': str,  # 67
    'primary_place_of_performance_code': str,  # 70
    'primary_place_of_performance_city_name': str,  # 71
    'prime_award_transaction_place_of_performance_cd_original': str,  # 77
    'prime_award_transaction_place_of_performance_cd_current': str,  # 78
    'primary_place_of_performance_county_code': convert_int_from_str,  # 64
    'primary_place_of_performance_county_name': str,  # 73
    'primary_place_of_performance_state_name': str,  # 75
    'prime_award_transaction_recipient_county_fips_code': str,  # 55
    'prime_award_transaction_place_of_performance_county_fips_code': str,  # 72
    'cfda_number': convert_float_from_str,  # 80
    # 'cfda_title': str  # 71
}

DW_CFDAs = [
    15.553,
    66.443,
    66.444,
    66.468,
    66.483,
    66.521,
    10.763,
    66.448
]

WW_CFDAs = [
    10.762,
    15.504,
    66.125,
    66.129,
    66.418,
    66.447,
    66.458,
    66.482
]

BOTH_DW_WW_CFDAs = [
    15.539,
    15.574,
    10.759,
    10.760,
    10.770,
    10.862,
    10.864,
    11.300,
    14.218,
    14.228,
    14.248,
    14.269,
    14.862,
    15.074,
    15.075,
    15.076,
    15.510,
    15.516,
    15.518,
    15.520,
    15.521,
    15.522,
    15.525,
    15.552,
    15.556,
    15.558,
    66.442,
    66.958,
    10.904,
    14.225,
    15.507,
    15.548
]
ALL_CFDAs = [
    15.539,
    15.574,
    10.759,
    10.760,
    10.762,
    10.770,
    10.862,
    10.864,
    11.300,
    14.218,
    14.228,
    14.248,
    14.269,
    14.862,
    15.074,
    15.075,
    15.076,
    15.504,
    15.510,
    15.516,
    15.518,
    15.520,
    15.521,
    15.522,
    15.525,
    15.552,
    15.553,
    15.556,
    15.558,
    66.125,
    66.129,
    66.418,
    66.442,
    66.443,
    66.444,
    66.447,
    66.458,
    66.468,
    66.482,
    66.483,
    66.521,
    66.958,
    10.763,
    10.904,
    14.225,
    15.507,
    15.548,
    66.448
]

prime_CFDAs = [
    15.539,
    15.574,
    10.759,
    10.760,
    10.762,
    10.770,
    10.862,
    10.864,
    15.074,
    15.075,
    15.076,
    15.504,
    15.510,
    15.516,
    15.518,
    15.520,
    15.521,
    15.522,
    15.525,
    15.552,
    15.553,
    15.556,
    15.558,
    66.125,
    66.129,
    66.418,
    66.442,
    66.443,
    66.444,
    66.447,
    66.521,
    66.958,
    10.763,
    10.904,
    14.225,
    15.507,
    15.548,
    66.448
]

# List of states
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





