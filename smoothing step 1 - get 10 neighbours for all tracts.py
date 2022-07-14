__author__ = "Wanmeng Liu"
_date_ = "2022-07-07"

'''



'''

import pandas as pd
##1. get tract level ruca & urban data only
p3 = '/Users/mercuryliu/Documents/MH Research/MH prevalence/Census tract data/Tract_to_ruca/'
ruca_tp = {'county_code_5': 'object', 'state_cd': 'object', 'geoid': 'object', 'primary_ruca_2010': 'int64', 'category': 'object'}
ruca_data = pd.read_csv(p3 + 'ruca_category.csv', dtype=ruca_tp)
urban_all = ruca_data[ruca_data['category'] == 'urban']
suburban_all = ruca_data[ruca_data['category'] == 'suburban']
rural_all = ruca_data[ruca_data['category'] == 'rural']




##2. get urban tracts' neighbors 
path2 = '/Users/mercuryliu/Documents/MH Research/MH prevalence/Census tract data/NBER_tract_distance/' 
nb_tp =  {'county1': 'object', 
'tract1': 'object', 
'mi_to_tract': 'float64',
 'county2': 'object', 
 'tract2': 'object', 'state': 'object', 
 'geoid_1': 'object', 'geoid_2': 'object'} 


dist_im = pd.read_csv(path2 + '25miles.csv', dtype=nb_tp, usecols=[5, 6,7,2])
dist_im.columns = ['mi_to_tract', 'state', 'geoid_i', 'geoid_m']




ruca_i = ruca_data[['geoid', 'primary_ruca_2010']]
ruca_i.columns = ['geoid_i', 'ruca_i']

ruca_m = ruca_data[['geoid', 'primary_ruca_2010']]
ruca_m.columns = ['geoid_m', 'ruca_m']

dist_im = dist_im.merge(ruca_i, how='left', on='geoid_i')
dist_im = dist_im.merge(ruca_m, how='left', on='geoid_m')
dist_im = dist_im.loc[(dist_im['ruca_i'] != 99) & (dist_im['ruca_m'] != 99)]

dist2 = dist_im.groupby(['geoid_i']).size().reset_index()
dist2.columns = ['geoid_i', 'neighbour_count']
dist_im = dist_im.merge(dist2, how='left', on='geoid_i')
dist_im = dist_im[dist_im['neighbour_count'] >= 10]
dist_im = dist_im.drop(columns=['neighbour_count'], axis=1)

need_100 = dist2.loc[dist2['neighbour_count'] < 10]
need_100 = pd.DataFrame(need_100['geoid_i'])





other_i = pd.read_csv(path2 + 'tracts_w_neighbours100miles.csv', dtype={'mi_to_tract':'float64',
'state': 'int64', 'geoid_i': 'object', 'geoid_m':'object' })

other_i = other_i.merge(ruca_i, how='left', on='geoid_i')
other_i = other_i.merge(ruca_m, how='left', on='geoid_m')
other_i = other_i.loc[(other_i['ruca_i'] != 99) & (other_i['ruca_m'] != 99)]


dist_all = pd.concat([dist_im, other_i])

dist_all['ruca_i'].unique()


dist_all["rank"] = dist_all.groupby("geoid_i")["mi_to_tract"].rank("average", ascending=True)
dist_all = dist_all[dist_all['rank'] <= 10]
all_test = dist_all.groupby(['geoid_i']).size().reset_index()
all_test.columns=['id', 'count']
all_test.loc[all_test['count']!= 10]



def ruca_3(row):
    if row['ruca_i'] in {1, 2, 3}:
        return 'urban'
    elif row['ruca_i'] in {4, 5, 6}:
        return 'suburban'
    else:
        return 'rural'

dist_all['cat_i'] = dist_all.apply(lambda row: ruca_3(row), axis=1)


#add poverty data
p4 ="/Users/mercuryliu/Documents/MH Research/MH prevalence/Census tract data/Tract_poverty/"

pov_tp = {'tract_id': 'object', 'st_key': 'int64', 'pov_child_pct': 'float64'}
pov = pd.read_csv(p4 + 'pov_pct.csv', dtype=pov_tp)

pov = pov[['tract_id', 'pov_child_pct']]

pov_i = pov.copy()
pov_i.columns = ['geoid_i', 'pov_child_pct_i']

pov_m = pov.copy()
pov_m.columns = ['geoid_m', 'pov_child_pct_m']
dist_all = dist_all.merge(pov_i, how='left', on='geoid_i')
dist_all = dist_all.merge(pov_m, how='left', on='geoid_m')

urban_final = dist_all.loc[dist_all['cat_i'] == 'urban']
suburban_final = dist_all.loc[dist_all['cat_i'] == 'suburban']
rural_final = dist_all.loc[dist_all['cat_i'] == 'rural']

dist_all.dtypes.apply(lambda x: x.name).to_dict()


urban_final.to_csv(p3 + 'urban_all.csv', header=True, index=False)
suburban_final.to_csv(p3 + 'suburban_all.csv', header=True, index=False)
rural_final.to_csv(p3 + 'rural_all.csv', header=True, index=False)

# tracts w/o neighbours within 25 files, we will look for their neighbours within 100 miles
#but 100 miles file is really big

import dask.dataframe as dd

dist_100 = dd.read_csv(path2 + '100miles.csv', dtype=nb_tp, usecols=[5,6,7,2] )
dist_100.head()
dist_100.columns=['mi_to_tract', 'state', 'geoid_i', 'geoid_m']
need_joined = dist_100.merge(need_100, how='inner', on='geoid_i')

need_joined2 = need_joined.compute()
need_joined2['geoid_i'].nunique()

need_joined2.to_csv(path2 + 'tracts_w_neighbours100miles.csv', header=True, index=False)