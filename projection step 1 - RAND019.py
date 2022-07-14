
## part1. RANDO19
import pandas as pd
import matplotlib.pyplot as plt

#use children population to specify weight, RAND019 approach

#1. get population estimate in each census tract
p3 = '/Users/mercuryliu/Documents/MH Research/MH prevalence/Census tract data/RAND019/'
a = ['GEO_ID', 'NAME', 'S0101_C01_002E', 'S0101_C01_003E', 'S0101_C01_004E', 'S0101_C01_005E']

p_tract = pd.read_csv(p3 + 'pop_tract.csv', usecols = a)

def tract_clean(df):
    df.columns = ['tract_id', 'name', 'a5', 'a59', 'a1014', 'a1519']
    df = df.iloc[1:]
    nm = ['tract_id', 'name', 'a5', 'a59', 'a1014', 'a1519']
    for i in range(2, 6):
        df[nm[i]] = pd.to_numeric(df[nm[i]], errors='coerce')
    
    df['pop_019_tract'] = df['a5'] +  df['a59'] +  df['a1014'] +  df['a1519'] 
    df['tract_id'] = df['tract_id'].astype('str').str[-11:]
    df = df[['tract_id', 'pop_019_tract']]
    return df

p_tract = tract_clean(p_tract)
p_tract.head()
len(p_tract)
p_tract['tract_id'].nunique()
#2. get population estimate in each zip code
def zip_clean(df):
    df.columns = ['zip_code', 'name', 'a5', 'a59', 'a1014', 'a1519']
    df = df.iloc[1:]
    nm = ['zip_code', 'name', 'a5', 'a59', 'a1014', 'a1519']
    for i in range(2, 6):
        df[nm[i]] = pd.to_numeric(df[nm[i]], errors='coerce')
    
    df['pop_019_zip'] = df['a5'] +  df['a59'] +  df['a1014'] +  df['a1519'] 
    df['zip_code'] = df['zip_code'].astype('str').str[-5:]
    df = df[['zip_code', 'pop_019_zip']]
    return df
p_zip = pd.read_csv(p3 + 'pop_zip.csv', usecols = a)
p_zip = zip_clean(p_zip)
p_zip.head()
p_zip.info()
p_zip['zip_code'].nunique()
len(p_zip)
len(zip_2_t)


#3. determine how many youth population a tract contributed to a zip
zip_2_t = pd.read_csv(p3 + 'zip_2_tract.csv', usecols= [0, 1])
tract_2_z = pd.read_csv(p3 + 'tract_2_zip.csv', usecols= [0, 1, 2])

zip_2_t.columns = ['zip_code', 'tract_id']
zip_2_t['zip_code'] = zip_2_t['zip_code'].astype('str').str.zfill(5)
zip_2_t['tract_id'] = zip_2_t['tract_id'].astype('str').str.zfill(11)
zip_2_t['tract_id'].nunique()

pop_zip_t = zip_2_t.merge(p_zip, how='left', on='zip_code')
len(pop_zip_t)
pop_zip_t = pop_zip_t.fillna(0)
pop_zip_t.info()


tract_2_z.columns = ['tract_id', 'zip_code', 'residential_ratio']
tract_2_z['zip_code'] = tract_2_z['zip_code'].astype('str').str.zfill(5)
tract_2_z['tract_id'] = tract_2_z['tract_id'].astype('str').str.zfill(11)
len(tract_2_z)
tract_2_z_pop = tract_2_z.merge(p_tract, how='left', on='tract_id')
tract_2_z_pop = tract_2_z_pop.fillna(0)
tract_2_z_pop['pop_019_tract_in_zip'] = tract_2_z_pop['pop_019_tract'] * tract_2_z_pop['residential_ratio']
#evidence: HCUP tract to zip covers more zip code than 2016 ACS estimate 
tract_2_z_pop['zip_code'].nunique() < len(p_zip)
#4. determine the tract weight for each tract-zip pair
weight_df = tract_2_z_pop.merge(p_zip, how='left', on='zip_code')
weight_df = weight_df.fillna(0)
weight_df['rand019_weight'] = weight_df['pop_019_tract_in_zip'] / weight_df['pop_019_zip']
weight_df.to_csv(p3 + 'weight_tract.csv', header=True, index=False)
weight_df.dtypes
weight_df.dtypes.apply(lambda x: x.name).to_dict()
w_tp = {'tract_id': 'object', 'zip_code': 'object', 'residential_ratio': 'float64', 'pop_019_tract': 'float64', 'pop_019_tract_in_zip': 'float64', 'pop_019_zip': 'float64', 'rand019_weight': 'float64'}