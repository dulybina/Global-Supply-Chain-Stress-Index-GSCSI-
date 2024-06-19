# -*- coding: utf-8 -*-
"""
Created on Sun Sep 26 13:50:55 2021
@author: Daria Ulybina
"""

import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import datetime
pd.set_option('display.max_columns', 500)
#pd.set_option('display.width', 1000)
pd.set_option("display.precision", 3)
pd.set_option("display.expand_frame_repr", False)
import glob
#import pdb

def get_latest_file():
    """Extract the files with the latest date with the name following the pattern: Saved_data_with_missing_"""
    list_of_files = glob.glob('Y:\\mt\\Saved_data_with_missing_*') # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file
        
def define_datetime():
    """Create timestamp extension FORMAT: _DDMMMYYYY"""
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%d%b%Y")
    assert isinstance(timestampStr, str)
    datetime_ext = "_"+str(timestampStr)
    return datetime_ext

def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

def plot_median_mean(nawc, traffic_type,location):
    """ Plot time series chart for North America west coast,
    median time in hours from departure from origin port
    to departure from destination port in NAWC"""
    
    for a in ['diff_hrs median','diff_hrs mean']:
        n = a.split(" ")[1].capitalize()
        assert isinstance(n, str)
        nawc.set_index('Departure_YearMonth')[a].plot()
        plt.ylabel("Hours")
        plt.xlabel("YearMonth departure from destination") 
        plt.title(f"{n} time difference for all trips\n ending with departure from {location} ({ traffic_type})")
        plt.tight_layout() 
        #plt.savefig(f"{n}_nawc_delay_({traffic_type[:3].lower()}).png" dpi=400)
        plt.show()
    return nawc
    
def get_data(filename):
    """Load main dataframe from serialized file, 
    make sure the timestamp column has correct data type 
    and create TRAFFIC_TYPE varaibles based on SHIP_CLASS_NAME"""
    df = pd.read_pickle(filename)
    df['TRAFFIC_TYPE']=df['SHIP_CLASS_NAME'].apply(lambda x: 'REGIONAL' if x in ['FEEDER', 'FEEDERMAX','HANDYSIZE','SMALL FEEDER'] else ('GLOBAL' if x in ['POST PANAMAX','PANAMAX','NEW PANAMAX','ULCV'] else 'nan'))

    df['Datetime'] = pd.to_datetime(df['TIMESTAMP_UTC']).dt.tz_localize(None)
    return df

def get_metadata(df):
    """
    Load metadata on ports (external file),
    derive ship-specific information from current working dataframe 
    and derive two additional port tables with prefilled headers for previous 
    and current ports. Make sure column types match to facilitate merging of dataframes
    """
    dfports=pd.read_csv('ports.csv', usecols=['PORT_ID', 'PORT_NAME', 'Latitude','Longitude', 'un_code', 'country_3', 'Economy name','Maritime_Region'])
    ship_table = df.groupby(by='SHIP_ID').agg({'MMSI': 'first', 'IMO': 'first','SHIPNAME':'first',
                                              'LENGTH':'max', 'WIDTH':'max', 'DWT':'max', 'GROSS_TONNAGE':'max',
                                               'TEU':'max','SHIP_CLASS_NAME':'first'}).reset_index()
    dfports['PORT_ID'] = dfports['PORT_ID'].astype(int)
    ship_table['SHIP_ID'] = ship_table['SHIP_ID'].astype(int)
    # Create previous and current  moves table headers (for further merging)
    prev = dfports.copy()
    cols = [x+"_PREV" for x in prev.columns]
    prev.columns = cols
    cur = dfports.copy()
    cols = [x+"_CUR" for x in cur.columns]
    cur.columns = cols
    prev['PORT_ID_PREV'] =prev['PORT_ID_PREV'].astype(int)
    cur['PORT_ID_CUR'] = cur['PORT_ID_CUR'].astype(int)
    return dfports, ship_table, prev, cur

def sequential_filter(df, traffic_type):
    """
    Function to eliminate spans of erroneous consequitive observations due
    to geofencing issues around terrestrial AIS receivers 
    """
    ## subset of all departures
    if traffic_type:
        dep = df[(df['MOVE_TYPE']=='DEPARTURE') & (df['TRAFFIC_TYPE']==traffic_type)]
    else:
         dep = df[(df['MOVE_TYPE']=='DEPARTURE')]
         
    dep = dep.copy()
    # sorting before the loop (just in case...)
    dep.sort_values(by=['SHIP_ID','TIMESTAMP_UTC'],inplace=True)
    print("Proportion of filtered observations (Traffic type + departures) ",len(dep)/len(df))
    
    #forming a group of observations for each ship to iterate over
    g_dep = dep.groupby(['SHIP_ID','IMO'])
    
    new_list = []
    new_accum = 0
    
    for g, v in g_dep:
        if len(v)>1:
            v = v.sort_values(by=['TIMESTAMP_UTC'], ascending=True)
            v['value_grp'] = (v['PORT_ID'] != v['PORT_ID'].shift()).cumsum()
            new = pd.DataFrame({'FirstDate' : v.groupby('value_grp')['TIMESTAMP_UTC'].first().values, 
                                'FirstDraft' : v.groupby('value_grp')['DRAUGHT_METERSX10'].first().values,
                                'LastDraft' : v.groupby('value_grp')['DRAUGHT_METERSX10'].last().values,
                                'LastDate' : v.groupby('value_grp')['TIMESTAMP_UTC'].last().values,
                                'Consecutive' : v.groupby('value_grp').size().values, 
                                'PORT_ID' : v.groupby('value_grp')['PORT_ID'].first().values}).reset_index(drop=True)
            new['SHIP_ID'] = g[0]
            new['IMO'] = g[1]
            new_accum += len(new)
            new_list.append(new)
        else:
            continue
        
    print(new_accum)
    deps = pd.concat(new_list)
    print("Should be close to 1: ", len(deps)/len(dep))
    return deps

#class Ports():E
  #"""tbc create a port class that would have a property dupsplit() duplcate split that would be used to create origin ports lookup table and destination ports lookup table
  #"""

def clean_deps(deps,prev,cur, ship_table):
    """
    Function to generate previous port visit values, involves shifting grouped observations backby one. From here on,
    extensions in variables _CUR for Current and _PREV for Previous port vistis
    It also merges shifted dataframes with the original table so that the the date and location of previous port
    visit are displayed on one single line. Final step - generate traffic type based on ship classes and drop pairless observations generated
    as a result of using .shift() method
    """
    deps['date'] = pd.to_datetime(deps['LastDate'])
    data = deps[['SHIP_ID','PORT_ID','date']].copy()
    data.rename(columns={'SHIP_ID':'SHIP_ID',
                         'PORT_ID':'PORT_CUR',
                         'date':'DATE_CUR'},inplace=True)

    data = data.sort_values(by = ['SHIP_ID', 'DATE_CUR'], ascending=[True,True])
    data['PORT_PREV'] = data.groupby(['SHIP_ID'])['PORT_CUR'].transform(lambda x:x.shift())
    data['DATE_PREV'] = data.groupby(['SHIP_ID'])['DATE_CUR'].transform(lambda x:x.shift())
    #data['TEU_PREV'] = data.groupby(['SHIP_ID'])['TEU'].transform(lambda x:x.shift())
    p1 = pd.merge(data,prev, how='left', left_on="PORT_PREV", right_on="PORT_ID_PREV")#, suffixes=("","_PREV"))
    p2 = pd.merge(p1,cur, how='left', left_on="PORT_CUR", right_on="PORT_ID_CUR")
    out = pd.merge(p2, ship_table, how='left', left_on='SHIP_ID',right_on='SHIP_ID')
    out['TRAFFIC_TYPE']=out['SHIP_CLASS_NAME'].apply(lambda x: 'REGIONAL' if x in ['FEEDER', 'FEEDERMAX', 'HANDYSIZE','SMALL FEEDER'] else ('GLOBAL' if x in ['POST PANAMAX','PANAMAX','NEW PANAMAX','ULCV'] else 'nan'))
    #data['TEU_PREV'] = data.groupby(['SHIP_ID'])['TEU'].transform(lambda x:x.shift())
    new = out.dropna(subset=['PORT_ID_PREV', 'PORT_ID_CUR'])
    return new

def time_difference(dataframe):
    """Derive time difference (seconds, hours) between two departures from consec. visited ports"""
    df =dataframe.copy()
    df['DATE_CUR'] = pd.to_datetime(df['DATE_CUR']).dt.tz_localize(None)
    df['DATE_PREV'] = pd.to_datetime(df['DATE_PREV']).dt.tz_localize(None)
    df['year'] = df['DATE_CUR'].dt.year
    df['month'] = df['DATE_CUR'].dt.month
    df['day'] = df['DATE_CUR'].dt.day
    #df['week_dep']=df['DATE_CUR'].dt.year.astype(str) + "-"+ df['DATE_CUR'].dt.isocalendar().week.astype(str).str.zfill(2)
    df['Departure_YearMonth'] = df['DATE_CUR'].dt.year.astype(str) + "-"+df['DATE_CUR'].dt.month.astype(str).apply(lambda x: x.zfill(2))
    df['week'] =df['DATE_CUR'].dt.normalize()-df['DATE_CUR'].dt.weekday.astype('timedelta64[D]')
    df= df.copy()
    df['delta_seconds']  = (df['DATE_CUR'] - df['DATE_PREV']).dt.total_seconds()
    df['diff_hrs'] = df['delta_seconds']/3600
    return df

def advanced_tdiff(df):
    """ Generate dates and derive time difference values between two consec. events due to some chances of seeing repeating erronous observations"""
    for c in ['DATE_CUR','DATE_PREV','LastDate_ar','LastDate_dep','FirstDate_ar','FirstDate_dep']:
        df[c] = pd.to_datetime(df[c]).dt.tz_localize(None)
    df['voyage_hrs_total']  = ((df['LastDate_dep'] - df['DATE_PREV']).dt.total_seconds())/3600
    df['voyage_hrs_without_wait']  = ((df['FirstDate_ar'] - df['DATE_PREV']).dt.total_seconds())/3600
    df['wait_turnaround_hrs']  = ((df['LastDate_dep'] - df['FirstDate_ar']).dt.total_seconds())/3600
    return df

def ports_dupsplit(dfports):
    """Generate two adidtional dataframe (for origins and destinations) with port-specific attributes """
    dfports = dfports[['PORT_ID', 'PORT_NAME',  'Latitude','Longitude', 'un_code', 'country_3','Latitude','Longitude']]
    dfports_orig = dfports.copy()
    dfports_dest = dfports.copy()
    dfports_orig.columns = [x+" Origin" for x in dfports.columns]
    dfports_dest.columns = [x+" Destination" for x in dfports.columns]
    return dfports_dest, dfports_orig

def augment_to_ports_aggregates(df, dfports, freq):
    
    if freq == "monthly":
        df['Departure_YearMonth'] = df['DATE_CUR'].dt.year.astype(str) + "-"+df['DATE_CUR'].dt.month.astype(str).apply(lambda x: x.zfill(2))
        freq_var = 'Departure_YearMonth'
    else:
        print(freq)
        freq_var = 'week'
    frame = df.groupby(['PORT_CUR','PORT_PREV',freq_var]).agg({'SHIP_ID':['count'],'TEU':['sum'],'diff_hrs':['median']}).reset_index()
    frame.columns = [' '.join(col).strip() for col in frame.columns.values]
    frame.rename(columns={"PORT_CUR": "PORT_ID Destination", "PORT_PREV": "PORT_ID Origin"}, inplace=True)
    print("Size of original dataframe: ",len(frame))
    dfports_dest, dfports_orig = ports_dupsplit(dfports)
    
    m1 = pd.merge(frame,dfports_orig, how='left', left_on="PORT_ID Origin", right_on="PORT_ID Origin")
    print('Size of dataframe after merging on origin port: ', len(m1))
    m2 = pd.merge(m1,dfports_dest, how='left', left_on="PORT_ID Destination", right_on="PORT_ID Destination")
    print('Size of dataframe after merging on destination port: ', len(m2))
    return m2

###############################################################################
def main():

    if os.path.exists("Y:\\mt\\"):
        # Change the current working Directory    
        os.chdir("Y:\\mt\\")
        
    ## Declare traffic type level 
    traffic_type = 'GLOBAL' #other options: "REGIONAL" or False
    visual = True
    
    filename = get_latest_file()
    print("Processing file: {}".format(filename))
    
    df = get_data(filename)
    
    # Traffic type and TEU-ship dictionaries generation 
    df_traffic = df[['SHIP_ID','TRAFFIC_TYPE']].drop_duplicates(subset=['SHIP_ID'])
    df_traffic = df_traffic.groupby("SHIP_ID")['TRAFFIC_TYPE'].first().dropna().reset_index()
    #df_teu = df.dropna(subset=['SHIP_ID','TEU']).groupby(['SHIP_ID'])['TEU'].first().reset_index()
    #teudict = dict(zip(df_teu['SHIP_ID'], df_teu['TEU']))
    #trfdict = dict(zip(df_traffic['SHIP_ID'], df_traffic['TRAFFIC_TYPE']))

    dfports, ship_table, prev, cur = get_metadata(df)
    #df['DRAUGHT2'] = df['DRAUGHT'].fillna(df['DRAUGHT_METERSX10'])
    deps = sequential_filter(df, traffic_type )
    data_full = clean_deps(deps,prev,cur, ship_table)
    data = time_difference(data_full)
    
    if visual:
        data['diff_hrs'].hist(range=(0,1000))
        plt.show()
    
    #df = advanced_tdiff(data_full)
    
    ### If ship-level granular data is needed, uncomment section below to save intermediate df
    #data.to_pickle(autostamp("Dep2Dep_all_ships_agg{}".format(define_datetime()),".pkl"))
    
    # Monthly aggregates on port level
    result = augment_to_ports_aggregates(data, dfports, 'monthly')
    result.to_csv("Dep2Dep_{}_ports_monthly_agg{}.csv".format( traffic_type , define_datetime()))
    
    #Weekly aggregates on port level
    result = augment_to_ports_aggregates(data, dfports, 'weekly')
    result.to_csv("Dep2Dep_{}_ports_weekly_agg{}.csv".format( traffic_type , define_datetime()))
    


if __name__ == "__main__":
    print("Running script...")
    main()    

