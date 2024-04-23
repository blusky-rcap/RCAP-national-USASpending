import pandas as pd
from efc_functions import *


print('loading dfs')
primes_df = pd.read_csv('o_noSubs_nat7.csv')
subs_df = pd.read_csv('o_sub9_pivoted.csv')
eda_df = pd.read_csv('EDA_files2.csv')
#eda_df = pd.read_csv('EDA_data1.csv')
print('dfs loaded')
# nat_df['recipient_county_name'] = nat_df['recipient_county_name'].str.replace('\d+', '')
eda_df['recipient_city_name'] = eda_df['recipient_city_name'].str.replace(r'"', '')
eda_df['recipient_city_name'] = eda_df['recipient_city_name'].str.split(',').str[0]
eda_df['recipient_city_name'] = eda_df['recipient_city_name'].str.replace(r'.', '')

eda_df['primary_place_of_performance_state_name'] = eda_df['primary_place_of_performance_state_name'].apply(
    lambda x: best_match(x))

eda_df[['cfda_number', 'prime_cfda_title']] = eda_df['cfda_numbers_and_titles'].str.split(':', n=1, expand=True)
groupby_cols = [
    'recipient_city_name',
    # 'recipient_county_name',
    # 'recipient_zip_code',
    'primary_place_of_performance_state_name',
    'cfda_number'
]
#cfda_numbers_and_titles
eda_grpd_df = eda_df.groupby(groupby_cols)
eda_grpd_df.sum().reset_index().to_csv('eda_grouped1.csv')
eda_grpd_read = pd.read_csv('eda_grouped1.csv')
eda_pivoted_df = pd.pivot_table(eda_grpd_read, values=['total_obligated_amount'],
                    index=['recipient_city_name',
                           #'recipient_county_name',
                           #'recipient_zip_code',
                           'primary_place_of_performance_state_name'],
                    columns=['cfda_number'],
                    aggfunc=sum,
                    fill_value=0,
                    margins=True
                    ).reset_index()

eda_pivoted_df.to_csv('eda_pivoted2.csv')
eda_processed_df = pd.read_csv('eda_pivoted1.csv')

#combined_df = primes_df.add(subs_df).fillna(primes_df)
#combined_df = primes_df.append(subs_df)
#combined_df = primes_df.merge(subs_df, how='full')
combined_df = pd.concat([primes_df, subs_df, eda_processed_df], join='outer')

combined_df = combined_df.fillna(0)
grp_cols = ['recipient_city_name',
               #'recipient_county_name',
               #'recipient_zip_code',
               'primary_place_of_performance_state_name'
            ]
grpd_combined_df = combined_df.groupby(grp_cols)


grpd_combined_df.sum().reset_index().to_csv('o_combined7_primes_subs.csv')

'''final_df = pd.read_csv('o_combined5_primes_subs.csv')
final_df['WW'] = sum([final_df[x] for x in final_df.columns.values if x[1] in WW_CFDAs])
final_df['DW'] = sum([final_df[x] for x in final_df.columns.values if x[1] in DW_CFDAs])
final_df['BOTH'] = sum([final_df[x] for x in final_df.columns.values if x[1] in BOTH_DW_WW_CFDAs])
final_df['NEITHER'] = sum([final_df[x] for x in final_df.columns.values if x[1] in ALL_CFDAs and x[1] not in BOTH_DW_WW_CFDAs
                     and x[1] not in DW_CFDAs
                     and x[1] not in WW_CFDAs
                     ])
final_df.to_csv('o_prime_sub_eda_1.csv')'''

print('script complete.')

order_df = pd.read_csv('o_combined7_primes_subs.csv')
order_df = order_df.reindex(sorted(order_df.columns), axis=1)
order_df.to_csv('o_combined_ordered1.csv')
