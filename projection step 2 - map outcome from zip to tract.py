import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn'

p3 = '/Users/mercuryliu/Documents/MH Research/MH prevalence/Census tract data/RAND019/'
w_tp = {'tract_id': 'object', 'zip_code': 'object', 'residential_ratio': 'float64', 'pop_019_tract': 'float64', 'pop_019_tract_in_zip': 'float64', 'pop_019_zip': 'float64', 'rand019_weight': 'float64'}
weight_df = pd.read_csv(p3 + 'weight_tract.csv',dtype = w_tp)

def get_weight_data(weight_df, st_cd):
    weight_df['state'] = weight_df['tract_id'].str[:2].astype('int')
    fips_dict = {'AK': 2, 'AL': 1,  'AZ': 4, 'CA': 6, 'CO': 8,  'DC': 11, 'DE': 10, 'FL': 12, 'GA': 13,  'HI': 15, 'IA': 19, 'ID': 16, 'IL': 17, 'IN': 18, 'KS': 20, 'KY': 21, 'LA': 22, 'MA': 25, 'MD': 24, 'ME': 23, 'MI': 26, 'MN': 27, 'MO': 29, 'MS': 28, 'MT': 30, 'NC': 37, 'ND': 38,  'NH': 33, 'NM': 35,  'NY': 36, 'OH': 39, 'OK': 40, 'OR': 41, 'PA': 42, 'PR': 72, 'RI': 44, 'SC': 45, 'SD': 46, 'TN': 47, 'TX': 48, 'UT': 49, 'VA': 51,  'WA': 53, 'WI': 55, 'WV': 54}
    st_key = fips_dict.get(st_cd)
    weight_df  = weight_df.loc[weight_df['state'] == st_key][['tract_id', 'zip_code', 'rand019_weight']]
    weight_df = weight_df.dropna()
    return weight_df

def outcome_zip_tract(path, input, output, wt_df):
    outcome = pd.read_csv(path + input, sep=',')
    outcome['zip_code'] = outcome['zip_code'].astype('str').str.zfill(5)
    outcome['state_cd'] = outcome['state_cd'].astype('str')
    outcome['YR_NUM'] = outcome['YR_NUM'].astype('str')
    outcome = outcome.rename(columns={'sum_children_count':'medicaid_child_CMS_zip', 'outcome1_count':'outcome1_count_zip'})
    ot1 = outcome.groupby(['state_cd', 'zip_code']).mean().reset_index()
    #some zip codes do not have corresponding census tract
    ot_zt1 = ot1.merge(wt_df, how='inner', on='zip_code')
    #print(ot_zt1.head())
    #print(ot_zt1.info())
    ot_zt1 = ot_zt1.fillna(0)
    ot_zt1['outcome1_prevalence_zip'] = ot_zt1['outcome1_count_zip'] / ot_zt1['medicaid_child_CMS_zip']
    ot_zt1['outcome1_prevalence_tract'] = ot_zt1['outcome1_prevalence_zip'] * ot_zt1['rand019_weight']
    ot_zt1 = ot_zt1.fillna(0)
    ot_zt1_for_smoothing1 = ot_zt1.groupby(['state_cd', 'tract_id'])['outcome1_prevalence_tract'].sum().reset_index()
    ot_zt1_for_smoothing1 = ot_zt1_for_smoothing1.fillna(0)
    ot_zt1_for_smoothing1 = ot_zt1_for_smoothing1.loc[ot_zt1_for_smoothing1['tract_id'] != 0]
    ot_zt1_for_smoothing1.to_csv(path + output, header = True, index=False)
    print(output, ' exported.')

all_st = ['CA', 'NY', 'WA', 'VA', 'AL', 'AZ', 'GA', 'LA', 'MA', 'MI', 'MS', 'NC', 'SC', 'TX', 'OH']

for i in range(len(all_st)):
    st_cd = all_st[i]
    input = 'outcome_final_by_zip_' + st_cd + '.csv'
    output = 'ot_tract_' + st_cd + '.csv'
    path = '/Users/mercuryliu/Documents/MH Research/MH prevalence/VM exported data/07082022 w 11/'

    wt_df = get_weight_data(weight_df, st_cd)
    outcome_zip_tract(path, input, output, wt_df)


