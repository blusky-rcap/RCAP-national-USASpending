# RCAP-national-USASpending
This repository contains Python scripts used to conduct analysis of USASpending data.
This project organizes federal spending using the Catalog of Federal Domestic Assistance (CFDA) numbers for each program. For additional details on the programs of interest for this project, see the 'Program Search_CFDAs.xlsx' spreadsheet in this repository.

## Preparation for Analysis
In order to conduct analysis, data must first be downloaded from USAspending.gov. There are a variety of ways to do this, such as the Award Data Archive, or the Custom Award Data download page. Choose a set of data to suit your needs, and be patient. Once downloaded, decompressed and organized, relevant paths, filenames, datatypes, and column indices can be updated throughout the code. 

Reference files are titled 'efc_functions.py,' and 'subs_functions.py.' Within these files, you will find lists of CFDA numbers, some relevant functions, a list of integers that represent the indices of columns of interest in the raw data, and a dict of column names and datatypes/simple conversion functions to pass to the object reading .csv data. Column indices, names, and datatypes should be re-confirmed each time new data is downloaded, since the format of the data is sometimes updated.

This project analyzed multiple types of awards, both Prime awards and Subawards. These datasets had to be downloaded separately. Files in this repository are named to distinguish whether they focus on prime or subawards. The scripts used for analyzing subawards are named with the prefix 'subaward_processing.' The numerical order of these files represents the order in which they should be run. The reference file for subaward analysis is 'subs_functions.py,' and it contains the indices and datatypes for the subaward columns (they do not match the prime award indices and column names/types).

