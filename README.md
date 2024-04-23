# RCAP-national-USASpending
## This repository contains Python scripts used to conduct analysis of USASpending data.

In order to conduct analysis, data must first be downloaded from USAspending.gov. Once decompressed and organized, relevant paths, filenames, datatypes, and column indices can be updated throughout the code. 

Reference files are titles 'efc_functions.py,' and 'subs_functions.py.' Within these files, you will find lists of CFDA numbers, some relevant functions, a list of integers that represent the indices of columns of interest in the raw data, and a dict of column names and datatypes/simple conversion functions to pass to the object reading .csv data. Upon undertaking any analysis of USAspending data, column indices and datatypes must be confirmed.
