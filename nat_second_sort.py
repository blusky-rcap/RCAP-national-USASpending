from efc_functions import *


def confirm_full_state_name(x):
    try:
        full_name = state_abbrev[x].upper()
        # print(x, ' turning into: ', full_name)
        return full_name
    except KeyError:
        print('hey its BAD ', x)


df = dd.read_csv('../DataFilteredByCFDA/*.csv', converters=dtypes_in)
df.to_csv('prime_CFDA_filtered2.csv', single_file=True)

nat_df = dd.read_csv('prime_CFDA_filtered2.csv', converters=dtypes_in, engine='python', on_bad_lines='warn')

nat_df['recipient_city_name'] = nat_df['recipient_city_name'].str.replace(r'"', '')
nat_df['recipient_city_name'] = nat_df['recipient_city_name'].str.split(',').str[0]
nat_df['recipient_city_name'] = nat_df['recipient_city_name'].str.replace(r'.', '')

nat_df['primary_place_of_performance_state_name'] = nat_df['primary_place_of_performance_state_name'].apply(
    lambda x: best_match(x))

print('checkinggggg ============================================')
for row in nat_df['primary_place_of_performance_state_name']:
    if type(row) == str and len(row) < 3:
        print(row)

groupby_cols = [
    'recipient_city_name',
    # 'recipient_county_name',
    # 'recipient_zip_code',
    'primary_place_of_performance_state_name',
    'cfda_number'
]

grpd = nat_df.groupby(groupby_cols)
grpd.sum().reset_index().to_csv('noSubs_nat7.csv', single_file=True)

print('saved')
