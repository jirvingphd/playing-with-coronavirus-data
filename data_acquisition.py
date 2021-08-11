from fsds.imports import *
import joblib

import os,zipfile,json
import functions as fn
DATA_FOLDER = "./data/"

### FUNCTIONS
def load_df_cases_ts(df_cases, id_cols = ['Province_State',"State_Code",'Admin2'],
                      var_name='date',value_name='# Cases',
                     cols_to_drop=['iso2','iso3','code3','UID',
                                   'Country_Region','Combined_Key',
                                   'Lat','Long_','FIPS']):
    
    value_cols = [c for c in df_cases.columns if c not in [*cols_to_drop,*id_cols]]
    
    df_cases_ts = pd.melt(df_cases, 
                          id_vars=id_cols, value_vars=value_cols,
                          var_name=var_name, value_name=value_name)
    
    df_cases_ts['date'] = pd.to_datetime(df_cases_ts['date'])
    df_cases_ts = df_cases_ts.set_index(['State_Code','date']).sort_index()
    return df_cases_ts


def load_df_deaths_ts(df_deaths, id_cols = ['Province_State','State_Code','Admin2'],
                      var_name='date',value_name='# Deaths',
                     cols_to_drop=['iso2','iso3','code3','UID',
                                   'Country_Region','Combined_Key',
                                   'Lat','Long_','FIPS','Population']):
    value_cols = [c for c in df_deaths.columns if c not in [*cols_to_drop,*id_cols]]
    df_deaths_ts = pd.melt(df_deaths,id_vars=id_cols, value_vars=value_cols,var_name=var_name,
                   value_name=value_name)
    df_deaths_ts['date'] = pd.to_datetime(df_deaths_ts['date'])

    df_deaths_ts = df_deaths_ts.set_index(['State_Code','date']).sort_index()
    return df_deaths_ts

# ## Getting Hospital Capacity Data
def get_hospital_data(verbose=True):
    from time import sleep
    offset = 0
    ## Getting Hospital Capacity Data
    base_url = 'https://healthdata.gov/resource/g62h-syeh.csv'
    page = 0
    results = []

    ## seting random, large page-len
    page_len = 1000

    while (page_len>0):
        try:
            if verbose:
                print(f"[i] Page {page}")
                print(f' - offset = {offset}')
            url = base_url+f"?$offset={offset}"
            df_temp = pd.read_csv(url)
            results.append(df_temp)

            page_len = len(df_temp)
            offset+=page_len
            page+=1
            sleep(0.3)
            
        except Exception as e:
            print(f"Error on page {page} with offset {offset}!")
            print(e)
            print("Returning List of Results in progress...")
            return results
        
    return pd.concat(results)


#### SCRIPT

## Make state abbreviation lookup
state_abbrevs = pd.read_csv('Reference Data/united_states_abbreviations.csv')

## Making dicts of Name:Abbrev and Abbrev:Name
state_to_abbrevs_map = dict(zip(state_abbrevs['State'],state_abbrevs['Abbreviation']))
abbrev_to_state_map = dict(zip(state_abbrevs['Abbreviation'],state_abbrevs['State']))

## Download Kaggle Data
os.system("kaggle datasets download -d antgoldbloom/covid19-data-from-john-hopkins-university")

jhu_data_zip = zipfile.ZipFile('covid19-data-from-john-hopkins-university.zip')

## prep metadata
file = 'CONVENIENT_us_metadata.csv'
jhu_data_zip.extract(file)
df_metadata = pd.read_csv(file)
df_metadata['State_Code'] = df_metadata['Province_State'].map(state_to_abbrevs_map)


## prep df_cases_ts
##  Prep Cases
file = 'RAW_us_confirmed_cases.csv'
jhu_data_zip.extract(file)

df_cases = pd.read_csv(file)
## Mapping State Abbrevs and Only Keep Matched States
df_cases['State_Code'] = df_cases['Province_State'].map(state_to_abbrevs_map)
df_cases = df_cases[df_cases['Province_State'].isin(state_abbrevs['State'])]

df_cases_ts = load_df_cases_ts(df_cases)


## Prep deaths
file = 'RAW_us_deaths.csv'
jhu_data_zip.extract(file)
df_deaths = pd.read_csv(file)

df_deaths['State_Code'] = df_deaths['Province_State'].map(state_to_abbrevs_map)
df_deaths = df_deaths[df_deaths['Province_State'].isin(state_abbrevs['State'])]

df_deaths_ts = load_df_deaths_ts(df_deaths)


## Prep Hospital Data
df1 = get_hospital_data()
df1['date'] = pd.to_datetime(df1['date'])

## Remnaming state columsn to match
df1 = df1.rename({'state':'State_Code'},axis=1)
df1['Province_State'] = df1['State_Code'].map(abbrev_to_state_map)
df1 = df1[df1['Province_State'].isin(state_abbrevs['State'])]
df1 = df1.sort_values(['Province_State','date'])
df1 = df1.drop_duplicates(keep='first')


## making df_hospitals
inpatient_bed_util_cols = [c for c in df1.columns if 'inpatient_beds_utilization' in c]
adult_icu_util_cols = [c for c in df1.columns if 'adult_icu_bed_utilization'in c]
KEEP_COLS = ['date','Province_State','State_Code',*inpatient_bed_util_cols,*adult_icu_util_cols]
df_hospitals = df1[KEEP_COLS].copy()
df_hospitals = df_hospitals.set_index(['State_Code','date']).sort_index()


### MAKE STATES DICT
unique_states = np.unique(df_hospitals.index.get_level_values(0))
len(unique_states)

STATES = {}

for state in unique_states:
    df_cases_temp = df_cases_ts.loc[state].sort_index().resample("D").sum().diff().fillna(0)
    df_deaths_temp = df_deaths_ts.loc[state].sort_index().resample("D").sum().diff().fillna(0)
    df_hospital_temp = df_hospitals.loc[state].drop(columns='Province_State').sort_index().resample("D").asfreq().ffill().fillna(0)
    
    df_state = pd.concat([df_cases_temp,df_deaths_temp,df_hospital_temp],axis=1).fillna(0)#.loc['03-2020':]
    df_state.to_csv(f"{DATA_FOLDER}combined_data_{state}.csv.gz",compression='gzip')
    STATES[state] = df_state.copy()
    
    
## Save final dict as joblib

joblib.dump(STATES,'data/STATE_DICT.joblib',compress=3)