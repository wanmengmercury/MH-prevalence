
import pandas as pd
import numpy as np
import statsmodels.api as sm

#### FUNCTIONS
def read(category):
    p3 = '/Users/mercuryliu/Documents/MH Research/MH prevalence/Census tract data/Tract_to_ruca/'

    sim_tp = {'mi_to_tract': 'float64', 'state': 'int', 'geoid_i': 'object', 'geoid_m': 'object', 'ruca_i': 'int64', 'ruca_m': 'int64', 'rank': 'float64', 'cat_i': 'object', 'pov_child_pct_i': 'float64', 'pov_child_pct_m': 'float64'}
    if category == 'urban':
        df = pd.read_csv(p3 + 'urban_all.csv', dtype = sim_tp)
    elif category == 'suburban':
        df = pd.read_csv(p3 + 'suburban_all.csv', dtype = sim_tp)
    else: 
        df = pd.read_csv(p3 + 'rural_all.csv', dtype = sim_tp)
    return df

def get_Dim(probability):
    if 0 < probability <= 0.1:
        return 1
    elif 0.1 < probability <= 0.2:
        return 2
    elif 0.2 < probability <= 0.3:
        return 3
    elif 0.3 < probability <= 0.4:
        return 4
    elif 0.4 < probability <= 0.5:
        return 5
    elif 0.5 < probability <= 0.6:
        return 6
    elif 0.6 < probability <= 0.7:
        return 7
    elif 0.7 < probability <= 0.8:
        return 8
    elif 0.8 < probability <= 0.9:
        return 9
    elif 0.9 < probability <= 1:
        return 10

def get_Pim(probability):
    if 0 < probability <= 0.1:
        return 1
    elif 0.1 < probability <= 0.2:
        return 2
    elif 0.2 < probability <= 0.3:
        return 3
    elif 0.3 < probability <= 0.4:
        return 4
    elif 0.4 < probability <= 0.5:
        return 5
    elif 0.5 < probability <= 0.6:
        return 6
    elif 0.6 < probability <= 0.7:
        return 7
    elif 0.7 < probability <= 0.8:
        return 8
    elif 0.8 < probability <= 0.9:
        return 9
    elif 0.9 < probability <= 1:
        return 10

#### PROCESSING starts
df = read('rural')
df['R_im'] = abs(df['ruca_i'] - df['ruca_m']) + 1

fips_dict = {'AK': 2, 'AL': 1,  'AZ': 4, 'CA': 6, 'CO': 8,  'DC': 11, 'DE': 10, 'FL': 12, 'GA': 13,  'HI': 15, 'IA': 19, 'ID': 16, 'IL': 17, 'IN': 18, 'KS': 20, 'KY': 21, 'LA': 22, 'MA': 25, 'MD': 24, 'ME': 23, 'MI': 26, 'MN': 27, 'MO': 29, 'MS': 28, 'MT': 30, 'NC': 37, 'ND': 38,  'NH': 33, 'NM': 35,  'NY': 36, 'OH': 39, 'OK': 40, 'OR': 41, 'PA': 42, 'PR': 72, 'RI': 44, 'SC': 45, 'SD': 46, 'TN': 47, 'TX': 48, 'UT': 49, 'VA': 51,  'WA': 53, 'WI': 55, 'WV': 54}
keys = list(fips_dict.keys())
values =list( fips_dict.values())

df_processed = pd.DataFrame(columns=['mi_to_tract', 'state', 'geoid_i', 'geoid_m', 'ruca_i', 'ruca_m',
       'rank', 'cat_i', 'pov_child_pct_i', 'pov_child_pct_m', 'R_im', 'D_im',
       'D_im_prob', 'pov_diff', 'P_im', 'P_im_prob', 'S_im'])
df_processed.to_csv('/Users/mercuryliu/Documents/MH Research/MH prevalence/Census tract data/Tract_to_ruca/rural_processed.csv', header=True, index=False, mode='a')  
pd.options.mode.chained_assignment = None


for i in range(len(values)):
    current_state = keys[i]
    df_state = df.loc[df['state'] == values[i]]
    #DISTANCE
    distance_sample = df_state['mi_to_tract'].tolist()
    #get empirical distribution of all possible distances 
    try:
        distance_emp_distri = sm.distributions.ECDF(distance_sample)
    except: 
        continue
    #calculate the probablity of each dim given the empirical distribution
    distance_prob  = distance_emp_distri(distance_sample)
    #divide the distance into 10 categories based on the probablity 
    Dim_list = [get_Dim(i) for i in distance_prob]
    df_state['D_im'] = Dim_list
    df_state['D_im_prob'] = distance_prob
    #POVERTY
    df_state['pov_diff'] = abs(df_state['pov_child_pct_i'] - df_state['pov_child_pct_m'])
    pov_sample = df_state['pov_diff'].tolist()
    pov_emp_distri = sm.distributions.ECDF(pov_sample)
    pov_prob  = pov_emp_distri(pov_sample)

    Pim_list = [get_Pim(i) for i in pov_prob]
    df_state['P_im'] = Pim_list
    df_state['P_im_prob'] = pov_prob
    #overall SIMILARITY 
    df_state['S_im'] = (df_state['R_im'] + df_state['P_im'] + df_state['D_im'] ) / 3
    df_state.to_csv('/Users/mercuryliu/Documents/MH Research/MH prevalence/Census tract data/Tract_to_ruca/rural_processed.csv', header=False, index=False, mode='a')  
    print(current_state + ' done.')

df_state.dtypes.apply(lambda x: x.name).to_dict()
