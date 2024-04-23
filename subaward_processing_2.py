from subs_functions import *


def confirm_full_state_name(x):
    try:
        full_name = state_abbrev[x].upper()
        #print(x, ' turning into: ', full_name)
        return full_name
    except KeyError:
        print('hey its BAD ', x)


def keyword_check(row):
    desc = row['subaward_description']
    print(type(row['prime_cfda']), ' ', row['prime_cfda'])
    print('t: ', type(desc), ' ', desc)
    if str(row['prime_cfda']) in SUBAWARD_cdbg_str:
        print('gotta keyword filter')
        for keyword in water_keywords:
            if keyword.upper() in desc:
                print(' KEEP ')
                return True
        print('DITCH')
    elif str(row['prime_cfda']) in SUBAWARD_srf_str:
        print('SRF entry')
        return True
    return False



nat_df = dd.read_csv('subs_filtered_CFDA_3.csv', converters=dtypes_in, engine='python', on_bad_lines='warn')

nat_df['subawardee_city_name'] = nat_df['subawardee_city_name'].str.replace(r'"', '')
nat_df['subawardee_city_name'] = nat_df['subawardee_city_name'].str.split(',').str[0]
nat_df['subawardee_city_name'] = nat_df['subawardee_city_name'].str.replace(r'.', '')


nat_df['subaward_primary_place_of_performance_state_name'] = nat_df['subaward_primary_place_of_performance_state_name'].apply(lambda x: best_match(x), meta=('subaward_primary_place_of_performance_state_name', str))

print('checkinggggg ============================================')

#df[df['menu_item'].str.contains('fresh|spaghetti')]
#water|pipe|drain|wastewater|sewer|sewage|drinking|sink|pump|storm|filtration|faucet|fountain|sump|lead|stormwater|water main|treatment plant|water wells|hydrant|plumping|sanitary
#df4[df4['col'].str.contains('|'.join(terms))]
#k_df = nat_df[nat_df['subaward_description'].str.contains('|'.join(water_keywords), na=False)]
nat_df['subaward_description'] = nat_df.subaward_description.astype(str)

m = nat_df.apply(keyword_check, axis=1)
new_df = nat_df[m]
#print(new_df)
#new_df.to_csv('applyTest2.csv', single_file=True)
#keyword_df = nat_df[nat_df['subaward_description'].str.contains('water', na=False)]
new_df.to_csv('keyword_filtered_subs4.csv', single_file=True)
print('sorted by keywords')
groupby_cols = [
                'subawardee_city_name',
                #'recipient_county_name',
                #'recipient_zip_code',
                'subaward_primary_place_of_performance_state_name',
                'prime_cfda'
               ]

print('now gonna group...')
grpd = new_df.groupby(groupby_cols)
print('group now saving')
grpd.sum().reset_index().to_csv('sub7.csv', single_file=True)

print('saved')
