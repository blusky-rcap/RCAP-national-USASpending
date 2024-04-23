from efc_functions import *

# read in list of cfda nums
#CFDA_list = read_csv('../EFC_CFDAs.txt')
#l = CFDA_list[1:]  # deal w 0 index issues
l = prime_CFDAs
# this comes in as a list of lists, no good, maybe fix read csv func sometime
cfda_l = []
# turn list of lists into list of nums
for entry in l:
    cfda_l.append(convert_float_from_str(entry))

# scan the directory that has the csv data
raw_csv_files = os.scandir(path='../AllTheData')
raw_filenames = []
# create list obj of file names from the os generated iterator
for file in raw_csv_files:
    raw_filenames.append(file.name)
print(raw_filenames)
#all_files = glob.glob('../AllTheData/*.csv')
print('READING IN DATA...')
i = 0
for datafile in raw_filenames:
    # print statements for debugging
    print(' reading file #: ', i, ', name: ', datafile)
    # read csv, must specify cols, types, etc
    df = dd.read_csv('../AllTheData/' + datafile, usecols=cols_in, converters=dtypes_in, engine='python', on_bad_lines='warn')
    # only keep cfda nums on the list
    filtered_df = df.loc[df['cfda_number'].isin(cfda_l)]
    print('filter -idk how to spell queued, saving...')
    # separate out string comprehension even tho who cares
    save_file = str(r'../DataFilteredByCFDA/' + str(i) + r'.csv')
    # save the file
    # note that each loop creates and saves a file before completing processing
    # (bc of delayed processing behavior in the code block)
    # so an incomplete file from the most recent loop may be left upon interrupt
    filtered_df.to_csv(save_file, single_file=True)
    print('saved file: ', i, '.csv')
    i += 1

print('DONE')

df = dd.read_csv('../DataFilteredByCFDA/*.csv', usecols=cols_in, converters=dtypes_in, engine='python', on_bad_lines='warn')
#nm_df = df.loc[df['primary_place_of_performance_state_name'] == 'New Mexico']
df.to_csv('prime_CFDA_filterd1.csv', single_file=True)
#nm_df.to_csv('NM_agged_CFDA_filtered.csv', single_file=True)
print('script complete.')
