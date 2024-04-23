# RCAP-national-USASpending
This repository contains Python scripts used to conduct analysis of USASpending data.
This project organizes federal spending using the Catalog of Federal Domestic Assistance (CFDA) numbers for each program. For additional details on the programs of interest for this project, see the 'Program Search_CFDAs.xlsx' spreadsheet in this repository.

## Preparation for Analysis
In order to conduct analysis, data must first be downloaded from USAspending.gov. There are a variety of ways to do this, such as the Award Data Archive, or the Custom Award Data download page. Choose a set of data to suit your needs, and be patient. Once downloaded, decompressed and organized, relevant paths, filenames, datatypes, and column indices can be updated throughout the code. 

Reference files are titled 'efc_functions.py,' and 'subs_functions.py.' Within these files, you will find lists of CFDA numbers, some relevant functions, a list of integers that represent the indices of columns of interest in the raw data, and a dict of column names and datatypes/simple conversion functions to pass to the object reading .csv data. Column indices, names, and datatypes should be re-confirmed each time new data is downloaded, since the format of the data is sometimes updated.

### Prime awards and Subawards
This project analyzed multiple types of awards, both Prime awards and Subawards. These datasets had to be downloaded separately. Files in this repository are named to distinguish whether they focus on prime or subawards. The scripts used for analyzing subawards are named with the prefix 'subaward_processing.' The numerical order of these files represents the order in which they should be run. The reference file for subaward analysis is 'subs_functions.py,' and it contains the indices and datatypes for the subaward columns (they do not match the prime award indices and column names/types).

## Code
For Prime award analysis, the first script that should be run is 'first_sort_cfdas.py,' followed by 'nat_second_sort.py' and then 'nat_third_sort.py.' For subaward analysis, the scripts should be run in numbered order. 

In the fourth subaward processing file, multiple dataframes are combined to produce a complete nation-wide table that incorporates both prime and subawards. In addition to the prime and subaward dataframes, there is an EDA dataframe. This references data for CFDA 11.3, which had to be handled separately because not all entries under the program are relevant, so the descriptions of each award needed to be filtered. This was also the case for some of the subawards, specifically the CDBG subawards (detailed in subs_functions.py). The EDA keyword filtering took place in excel, but the CDBG keyword filtering was handled in code, which can be seen in the second subaward processing file.
