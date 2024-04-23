from subs_functions import *

cfda_l = SUBAWARD_str
print(type(cfda_l[0]))
#numeric_const_pattern = '[-+]? (?: (?: \d* \. \d+ ) | (?: \d+ \.? ) )(?: [Ee] [+-]? \d+ ) ?'
#rx = re.compile(numeric_const_pattern, re.VERBOSE)
# rx.findall("Some example: Jr. it. was .23 between 2.3 and 42.31 seconds")

# scan the directory that has the csv data
raw_csv_files = os.scandir(path='../SubawardFiles')
raw_filenames = []
# create list obj of file names from the os generated iterator
for file in raw_csv_files:
    raw_filenames.append(file.name)

#all_files = glob.glob('../AllTheData/*.csv')
print('READING IN DATA...')
i = 0
for datafile in raw_filenames:
    # print statements for debugging
    print(' reading file #: ', i, ', name: ', datafile)
    # read csv, must specify cols, types, etc
    df = dd.read_csv('../SubawardFiles/' + datafile, usecols=cols_in, converters=dtypes_in, engine='python', on_bad_lines='warn')
    # only keep cfda nums on the list

    df[['prime_cfda', 'prime_cfda_title']] = df['prime_award_cfda_numbers_and_titles'].str.split(':', n=1, expand=True)

    filtered_df = df.loc[df['prime_cfda'].isin(cfda_l)]
    print('filter -idk how to spell queued, saving...')
    # separate out string comprehension even tho who cares
    save_file = str(r'../SubsFilteredByCFDA/' + str(i) + r'.csv')
    # save the file
    # note that each loop creates and saves a file before completing processing
    # (bc of delayed processing behavior in the code block)
    # so an incomplete file from the most recent loop may be left upon interrupt
    filtered_df.to_csv(save_file, single_file=True)
    print('saved file: ', i, '.csv')
    i += 1

print('DONE FILTERING')

df = dd.read_csv('../SubsFilteredByCFDA/*.csv')
df.to_csv('subs_filtered_CFDA_3.csv', single_file=True)

print('done combining to 1 output df')
