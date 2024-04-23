import pandas as pd
from efc_functions import *


nat_df = pd.read_csv('sub7.csv')
nat_dropped = nat_df.drop(columns=['prime_award_amount'])
new_col_names = {
    'subawardee_city_name': 'recipient_city_name',
    'subaward_primary_place_of_performance_state_name': 'primary_place_of_performance_state_name',
    'prime_cfda': 'cfda_number',
    'subaward_amount': 'total_obligated_amount'
}
nat_dropped.rename(columns=new_col_names, inplace=True)
# pivot the table so that CFDA nums are column names
p1 = pd.pivot_table(nat_dropped, values=['total_obligated_amount'],
                    index=['recipient_city_name',
                           #'recipient_county_name',
                           #'recipient_zip_code',
                           'primary_place_of_performance_state_name'],
                    columns=['cfda_number'],
                    aggfunc=sum,
                    fill_value=0,
                    margins=True
                    ).reset_index()

# save pivoted table
p1.to_csv('o_sub9_pivoted.csv')
print("saved")
