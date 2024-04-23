import pandas as pd
from efc_functions import *

nat_df = pd.read_csv('noSubs_nat7.csv')
nat_dropped = nat_df.drop(columns=['Unnamed: 0',
                                 #'total_obligated_amount',
                                 'federal_action_obligation',
                                 'non_federal_funding_amount',
                                 'total_non_federal_funding_amount',
                                 'original_loan_subsidy_cost',
                                 'total_face_value_of_loan',
                                 'total_loan_subsidy_cost',
                                 'total_outlayed_amount_for_overall_award',
                                 'action_date_fiscal_year'])



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

# values=['federal_action_obligation']
p1['WW'] = sum([p1[x] for x in p1.columns.values if x[1] in WW_CFDAs])
p1['DW'] = sum([p1[x] for x in p1.columns.values if x[1] in DW_CFDAs])
p1['BOTH'] = sum([p1[x] for x in p1.columns.values if x[1] in BOTH_DW_WW_CFDAs])
p1['NEITHER'] = sum([p1[x] for x in p1.columns.values if x[1] in ALL_CFDAs and x[1] not in BOTH_DW_WW_CFDAs
                     and x[1] not in DW_CFDAs
                     and x[1] not in WW_CFDAs
                     ])
p1.to_csv('o_noSubs_nat7.csv')
print("saved")
