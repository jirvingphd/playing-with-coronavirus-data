import pandas as pd
import numpy as np

# @add_method(CoronaData)
def download_coronavirus_data(path='New Data/',verbose=False):
    """Installs the Kaggle Command Line Interface to clone dataset.
    Then extracts dataset to specified path and displays name of main file.
    Args:
        path(str): Folder to extract dataset into (must end with a '/')
        
    Returns:
        file_list(list): List of full filepaths to downloaded csv files.
    """
    ## Determine if dataset is downloaded via Kaggle CL
    import os,glob
    from zipfile import ZipFile
    from IPython.display import clear_output
    os.makedirs(path, exist_ok=True)

    ## Install Kaggle 
    !pip install kaggle --upgrade
    clear_output()
    
    ## Run Kaggle Command 
    cmd = 'kaggle datasets download -d sudalairajkumar/novel-corona-virus-2019-dataset'
    os.system(cmd)
    
    ## Extract ZipFile
    print(f'Downloaded dataset Zipfie, extracting to {path}...')
    zip_filepath = 'novel-corona-virus-2019-dataset.zip'
    with ZipFile(zip_filepath) as file:
        file.extractall(path)
    
    ## Delete Zip File
    os.system(f"rm {zip_filepath}"  )
    
    
    ## Get list of all csvs
    print('[i] Extraction Complete.')    
    file_list = glob.glob(path+"*.csv")
    
    
    ## Find main df 
    main_file = [file for file in file_list if 'covid_19_data.csv' in file]
    if verbose:
        print(f"[i] The main file name is {main_file}")
    return main_file[0] #file_list[index]



# @add_method(CoronaData)
def load_raw_df(fpath,kws={},verbose=True):
    """Performs most basic of preprocessing, including renaming date column to 
    Date and dropping 'Last Update', and 'SNo' columns"""
    
    ## Default Kws
    read_kws = dict(parse_dates=['ObservationDate','Last Update'])
    
    ## Add User kws
    read_kws = {**read_kws,**kws}
    
    if verbose:
        print(f"[i] Loading {fpath} with read_csv kws:",end='')
        display(read_kws)
        
    df = pd.read_csv(fpath,**read_kws)
    
    ## Drop unwated columns
    df.drop(['Last Update',
             'SNo'],axis=1,inplace=True)

    ## Rename Date columns
    df.rename({'ObservationDate':'Date'},axis=1,inplace=True)
    
    ## Display some info 
    if verbose:
        display(df.head())
        # Countries in the dataset
        print(f"[i] There are {len(df['Country/Region'].unique())} countries in the datatset")
        
        ## Get first and last date
        start_ts = df["Date"].loc[df['Date'].idxmin()].strftime('%m-%d-%Y')
        end_ts = df["Date"].loc[df['Date'].idxmax()].strftime('%m-%d-%Y')
        # DF['Date'].idxmin(), DF['Date'].idxmax()
        print(f"[i] Dates Covered:\n\tFrom {start_ts} to {end_ts}")

    return df

# @add_method(CoronaData)
def set_datetime_index(df_,col='Date'):#,drop_old=False):
    df = df_.copy()
    df[col] = pd.to_datetime(df[col],infer_datetime_format=True)
    df.set_index(df[col],drop=True,inplace=True)
    if col in df.columns:
        df.drop(columns=col,inplace=True)
    return df#, inplace=True)


# @add_method(CoronaData)
def load_us_reference_info():
    ## Making Master Lookup CSV
    abbrev = pd.read_csv('Reference Data/united_states_abbreviations.csv')
    pop = pd.read_csv('Reference Data/us-pop-est2019-alldata.csv')
    us_pop = pop.loc[pop['STATE']>0][['NAME','POPESTIMATE2019']].copy()
    us_info = pd.merge(abbrev,us_pop,right_on='NAME',left_on='State',how="inner")
    us_info.drop('NAME',axis=1,inplace=True)
    return us_info


# @add_method(CoronaData)
def calculate_per_capita(df_,stat_cols = ['Confirmed','Deaths','Recovered']):
    df = df_.copy()
    ## ADDING PER CAPITA DATA 
    for col in stat_cols:
        df[f"{col} Per Capita"] = df[col]/df['POPESTIMATE2019']
    df.drop('POPESTIMATE2019',axis=1,inplace=True)
    return df


# @add_method(CoronaData)
def get_and_clean_US(df,save_as = 'Reference Data/united_states_abbreviations.csv',
                     make_date_index=True,per_capita=True):
    """Takes raw df loaded and extracts United States and processes
    all state names to create new abbreviation column 'state'.
    """
    ## Get only US
    df_us = df.groupby('Country/Region').get_group('US').copy() 
    
    # REPLACE STATE NAMES
    # https://raw.githubusercontent.com/jirvingphd/List-of-US-States/master/states.csv
#     try:
#         state_url = 'https://raw.githubusercontent.com/jirvingphd/List-of-US-States/master/states.csv'
#         state_lookup = pd.read_csv(state_url)
#         state_lookup.to_csv(save_as, index=False)
#     except:
#         print('Error downloading state lookup csv. Using local copy')
#         state_lookup.read_csv(save_as)
    state_lookup = load_us_reference_info()

    
    ## Make renaming dict for states
    STATE_DICT = dict(zip(state_lookup['State'],state_lookup['Abbreviation']))
    STATE_DICT.update({'Chicago':'IL',
                      'Puerto Rico':'PR',
                      'Virgin Islands':'VI',
                      'United States Virgin Islands':'VI'})

    ## Separately Process Rows that contain a city, state 
    df_city_states = df_us[df_us['Province/State'].str.contains(',')]
    
    
    ## Finding City Abbreviations in city_states
    import re
    state_expr = re.compile(r"[A-Z\.]{2,4}")
    df_city_states['state'] = df_city_states['Province/State'].apply(state_expr.findall)
    df_city_states = df_city_states.explode('state')


    ## Seperately process Rows that do not contain a city,state
    df_states = df_us[~df_us['Province/State'].str.contains(',')]
    df_states['state'] =  df_states['Province/State'].map(STATE_DICT)

    ## Combining data frame back together
    df = pd.concat([df_states,df_city_states]).sort_index()
    df = df.dropna(subset=['state'])
    
    ## Fix some stragglers (like D.C. vs DC)
    df['state'] = df['state'].replace('D.C.','DC')
    
    ## Add Population Data
    df = pd.merge(df, state_lookup,left_on='state',right_on="Abbreviation")
    
    if per_capita:

        for col in  ['Confirmed','Deaths','Recovered']:
            df[f"{col} Per Capita"] = df[col]/df['POPESTIMATE2019']
            
            ## Remove Population 
        df.drop('POPESTIMATE2019',axis=1,inplace=True)
#     if len(save_as)>0:
#         print(f'[i] Saving final df as {save_as}')
#         df.to_csv(save_as,index=False)
        
    if make_date_index:
        df = set_datetime_index(df)
    return df


def get_state_ts(df, state_name,
                     group_col='state', ts_col=None,
                     freq='D', agg_func='sum'):
    """Take df_us and extracts state's data as then Freq/Aggregation provided"""
    
    ## Get state_df group
    state_df = df.groupby(group_col).get_group(state_name)#.resample(freq).agg(agg)
    
    ## Resample and aggregate state data
    state_df = state_df.resample(freq).agg(agg_func)
    
    
    ## Get and Rename Sum Cols 
    orig_cols = state_df.columns

    ## Create Renamed Sum columns
    for col in orig_cols:
        state_df[f"{state_name} - {col}"] = state_df[col]
      
    ## Drop original cols
    state_df.drop(orig_cols,axis=1,inplace=True)
    
    if ts_col is not None:
        ts_cols_selected = [col for col in state_df.columns if ts_col in col]
        state_df = state_df[ts_cols_selected]

    return state_df



import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go


def plot_states(df, state_list, plot_cols = ['Confirmed'],df_only=False,
                new_only=False,plot_scatter=True,show=False):
    """Plots the plot_cols for every state in state_list.
    Returns plotly figure
    New as of 06/21"""
    
    ## Get state dataframes
    concat_dfs = []  
    STATES = {}
    
    ## Get each state
    for state in state_list:

        # Grab each state's df and save to STATES
        dfs = get_state_ts(df,state)
        STATES[state] = dfs

        ## for each plot_cols, find all columns that contain that col name
        for plot_col in plot_cols:
            concat_dfs.append(dfs[[col for col in dfs.columns if col.endswith(plot_col)]])#plot_col in col]])

    ## Concatenate final dfs
    plot_df = pd.concat(concat_dfs,axis=1)#[STATES[s] for s in plot_states],axis=1).iplot()
    
    
    ## Set title and df if new_only
    if new_only:
        plot_df = plot_df.diff()
        title = "Coronavirus Cases by State - New Cases"
    else:
        title = 'Coronavirus Cases by State - Cumulative'
    
    ## Reset Indes
    plot_df.reset_index(inplace=True)
    
    ## Return Df or plot
    if df_only==False:

        if np.any(['per capita' in x.lower() for x in plot_cols]):
            value_name = "# of Cases - Per Capita"
        else:
            value_name='# of Cases'
        pfig_df_melt = plot_df.melt(id_vars=['Date'],var_name='State',
                                    value_name=value_name)
        
        if plot_scatter:
            plot_func = px.scatter
        else:
            plot_func = px.line
        # Plot concatenated dfs
        pfig = plot_func(pfig_df_melt,x='Date',y=value_name,color='State',
                      width=800,height=500,title=title)
        
#         pfig.update_xaxes(rangeslider_visible=True)

                # Add range slider
        pfig.update_layout(
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=7,
                             label="1week",
                             step="day",
                             stepmode="backward"),
                        dict(count=14,
                             label="2weeks",
                             step="day",
                             stepmode="backward"),
                        dict(count=1,
                             label="1m",
                             step="month",
                             stepmode="backward"),
                        dict(count=6,
                             label="6m",
                             step="month",
                             stepmode="backward"),

                        dict(step="all")
                    ])
                ),
                rangeslider=dict(
                    visible=True
                ),
                type="date"
            )
        )
        
        if show:
            pfig.show()
            
        return pfig
    else:
        return plot_df#.reset_index()
    