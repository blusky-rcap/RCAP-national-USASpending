import csv
import json
import time
import zipfile
import pprint
import dask.dataframe as dd
import numpy as np
from glob import glob
from datetime import datetime


#  at the county level, who's received federal $ for given CFDA numbers

data_root = '/Users/bspitzer/Desktop/RCAP/USASpending_CSV_Analysis/CSV/FY*_All_Assistance_Full_20220*/FY' \
            '*_All_Assistance_Full_*.csv'

data_ex = '/Users/bspitzer/Desktop/RCAP/USASpending_CSV_Analysis/CSV/FY2011_All_Assistance_Full_20220208' \
          '/FY2011_All_Assistance_Full_20220208_1.csv '


def read_csv(file):
    arr = []
    with open(file, newline='', encoding="ISO-8859-1") as csvfile:
        reader = csv.reader(csvfile)
        # try:
        for row in reader:
            try:
                arr.append(row)
            except UnicodeDecodeError:
                print("decode error reading CSV: ", row)
                continue
    # except UnicodeDecodeError:
    # print("OUTER decode error reading CSV: ", row)

    csvfile.close()
    return arr


def convert_float_from_str(val):
    cfda = 0.00
    try:
        cfda = np.float64(val)
        return cfda
    except ValueError:
        cfda = 0.00
        return cfda


def convert_int_from_str(val):
    try:
        out = int(val)
        return out
    except ValueError:
        return 0
    except TypeError:
        return 0


def echo_str(val):
    return str(val)


#  MAIN

EFC_cfdas = read_csv("/Users/bspitzer/Documents/RCAP/USASpending_CSV_Analysis"
                     "/C_WA_WaterFunds_Allyrs_percap_ranked7_ByType (1).csv")
EFC_cfdas = EFC_cfdas[1:]  # idk why this is necessary there's some weird 0 index fuckery
cfda_l = []
for entry in EFC_cfdas:
    cfda_l.append(convert_float_from_str(entry[0]))

rel_columns_all = [7, 8, 9, 10, 18, 49, 50, 51, 53, 64, 65, 66, 70]  # pull columns with relevant keys from raw dataset on hard drive
col_types_all = {
    'total_obligated_amount': convert_float_from_str,  # 7
    'non_federal_funding_amount': convert_float_from_str,  # 8
    'total_non_federal_funding_amount': convert_float_from_str,  # 9
    'face_value_of_loan': convert_float_from_str,  # 10
    'action_date_fiscal_year': convert_int_from_str,  # 18
    'recipient_county_code': convert_int_from_str,  # 49
    'recipient_county_name': str,  # 50
    'recipient_state_code': str,  # 51
    'recipient_zip_code': convert_int_from_str,  # 53
    'primary_place_of_performance_county_code': convert_int_from_str,  # 64
    'primary_place_of_performance_county_name': str,  # 65
    'primary_place_of_performance_state_name': str,  # 66
    'cfda_number': convert_float_from_str,  # 70
    #'cfda_title': str  # 71
}

rel_columns_recipient = [7, 8, 9, 10, 18, 49, 50, 51, 53, 70]  # pull columns with relevant keys from raw dataset on hard drive
col_types_recipient = {
    'total_obligated_amount': convert_float_from_str,  # 7
    'non_federal_funding_amount': convert_float_from_str,  # 8
    'total_non_federal_funding_amount': convert_float_from_str,  # 9
    'face_value_of_loan': convert_float_from_str,  # 10
    'action_date_fiscal_year': convert_int_from_str,  # 18
    'recipient_county_code': convert_int_from_str,  # 49
    'recipient_county_name': str,  # 50
    'recipient_state_code': str,  # 51
    'recipient_zip_code': convert_int_from_str,  # 53
    'cfda_number': convert_float_from_str,  # 70
    #'cfda_title': str  # 71
}
#  plop = place of performance
rel_columns_plop = [7, 8, 9, 10, 18, 64, 65, 66, 70]  # pull columns with relevant keys from raw dataset on hard drive
col_types_plop = {
    'total_obligated_amount': convert_float_from_str,  # 7
    'non_federal_funding_amount': convert_float_from_str,  # 8
    'total_non_federal_funding_amount': convert_float_from_str,  # 9
    'face_value_of_loan': convert_float_from_str,  # 10
    'action_date_fiscal_year': convert_int_from_str,  # 18

    'primary_place_of_performance_county_code': convert_int_from_str,  # 64
    'primary_place_of_performance_county_name': str,  # 65
    'primary_place_of_performance_state_name': str,  # 66
    'cfda_number': convert_float_from_str,  # 70
    #'cfda_title': str  # 71
}

curr_file = data_ex
print("NOW READING...")

filename = '/Users/bspitzer/Documents/RCAP/USASpending_CSV_Analysis/CSV/FY2011_All_Assistance_Full_20220208' \
           '/FY2011_All_Assistance_Full_20220208_1.csv'
for i in range(2011, 2023):
    print("YEAR: ", i)
    file = '/Users/bspitzer/Documents/RCAP/USASpending_CSV_Analysis/CSV/FY' \
            + str(i) + '_All_Assistance_Full_20220*/*.csv'
    df = dd.read_csv(file, usecols=rel_columns_plop, converters=col_types_plop, engine='python', on_bad_lines='warn')

    EFC = df.loc[df['cfda_number'].isin(cfda_l)]
    print("EFC head: ", EFC.head())
    print("SORTED CFDA's")

    groupby_list_all = ['cfda_number', 'recipient_state_code', 'recipient_county_name', 'recipient_zip_code',
                          'action_date_fiscal_year', 'recipient_county_code', 'primary_place_of_performance_county_code', 'primary_place_of_performance_county_name', 'primary_place_of_performance_state_name']

    groupby_list_recipient = ['cfda_number', 'recipient_state_code', 'recipient_county_name', 'recipient_zip_code',
                          'action_date_fiscal_year', 'recipient_county_code', 'primary_place_of_performance_county_code', 'primary_place_of_performance_county_name', 'primary_place_of_performance_state_name']

    groupby_list_plop = ['cfda_number', 'action_date_fiscal_year', 'primary_place_of_performance_county_code', 'primary_place_of_performance_county_name', 'primary_place_of_performance_state_name']

    states = EFC.groupby(groupby_list_plop)  # change df to tnc and uncomment prev line to narrow down cfdas

    print("GROUPED")
    output_filename = 'plop_EFC_County_Level_FY' + str(i) + '.csv'
    states.sum().reset_index().to_csv(output_filename, single_file=True)
    print(i, " COMPLETE")


# error for 2016: Skipping line 63416: NULL byte detected. This byte cannot be processed in Python's native csv
# library at the moment, so please pass in engine='c' instead
# EFC2.groupby(['recipient_county_name', 'cfda_number', 'action_date_fiscal_year', 'recipient_county_code', 'recipient_state_code']).sum().to_csv('aggtest8.csv', single_file=True)