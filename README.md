# RCAP-national-USASpending
This repository contains Python scripts used to conduct analysis of USASpending data.

## Preparation for Analysis
In order to conduct analysis, data must first be downloaded from USAspending.gov. There are a variety of ways to do this, such as the Award Data Archive, or the Custom Award Data download page. Choose a set of data to suit your needs, and be patient. Once downloaded, decompressed and organized, relevant paths, filenames, datatypes, and column indices can be updated throughout the code. 

Reference files are titled 'efc_functions.py,' and 'subs_functions.py.' Within these files, you will find lists of CFDA numbers, some relevant functions, a list of integers that represent the indices of columns of interest in the raw data, and a dict of column names and datatypes/simple conversion functions to pass to the object reading .csv data. Column indices, names, and datatypes should be re-confirmed each time new data is downloaded, since the format of the data is sometimes updated.
