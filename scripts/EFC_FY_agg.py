import csv
import json
import time
import zipfile
import pprint
import dask.dataframe as dd
import numpy as np
from glob import glob
from datetime import datetime

aggd_FY_path = '/Users/bspitzer/Documents/RCAP/USASpending_CSV_Analysis/venv/2EFC_County_Level_FY*.csv'
output_filename = 'EFC_cfda_county_lvl_2.csv'

EFC = dd.read_csv(aggd_FY_path, engine='python', on_bad_lines='warn')

print("EFC head: ", EFC.head())

groupby_list = ['cfda_number', 'recipient_state_code', 'recipient_county_name', 'recipient_zip_code',
                          'action_date_fiscal_year', 'recipient_county_code', 'primary_place_of_performance_county_code', 'primary_place_of_performance_county_name', 'primary_place_of_performance_state_name']
# change df to tnc and uncomment prev line to narrow down cfdas

#states = EFC.groupby(['cfda_number', 'recipient_state_code', 'recipient_county_name',
#                      'action_date_fiscal_year'])  # change df to tnc and uncomment prev line to narrow down cfdas

#EFC.sum().reset_index().to_csv(output_filename, single_file=True)
#EFC.sum().to_csv(output_filename, single_file=True)
EFC.groupby(groupby_list).sum().to_csv('aggtest11.csv', single_file=True)