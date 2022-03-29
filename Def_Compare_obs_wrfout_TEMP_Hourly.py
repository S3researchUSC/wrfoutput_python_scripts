import pandas as pd
import numpy as np
from netCDF4 import Dataset
from datetime import datetime

def Compare_Hourly_T2(obser_dir,wrfout_dir,compare_dir,months):
    
    Pollu_obser_name = '2_Meteorology/1_TEMP_Hourly/1_AQS/TEMP_Hourly_2016_AQS_Processed_'
    Pollu_loc_name = '2_Meteorology/1_TEMP_Hourly/1_AQS/TEMPHourly_Locations_2016_AQS_'
    Pollu_wrfout_name = 'T2_hourly_2016' #dimension (ndays*nhour*nlat*nlon)

    #read in land use type
    lu_name = '/scratch/hschlaer/wrfinput_pouyaMODIS/wrfinput_d03_jul'
    lu_file = Dataset(lu_name,mode='r')
    lu_index = np.array(lu_file.variables['LU_INDEX'][0,:,:])


    for currmonth in list(months.keys()):
        Pollu_obser = pd.read_excel(obser_dir+Pollu_obser_name+months[currmonth][1]+'.xlsx',sheet_name='Sheet1',skiprows=0, header=0)
        print("pollutant observation is in:")
        print(obser_dir+Pollu_obser_name+months[currmonth][1]+'.xlsx')
        Pollu_obser['Date Local']=Pollu_obser['Date Local'].astype('str')
        Pollu_obser['Time Local']=Pollu_obser['Time Local'].astype('str')
        Pollu_loc = pd.read_excel(obser_dir+Pollu_loc_name+months[currmonth][1]+'.xlsx',sheet_name='Sheet1',skiprows=0, header=0)
        print("Hourly wrfout file is:")
        print(wrfout_dir+Pollu_wrfout_name+months[currmonth][1]+'.npy')
        try:
            Pollu_wrfout = np.load(wrfout_dir+Pollu_wrfout_name+months[currmonth][1]+'.npy')
            print(wrfout_dir+Pollu_wrfout_name+months[currmonth][1]+'.npy')
        except:
            continue
        #add wrfout column to observation dataframe
        num_row = len(Pollu_obser.index)
        init_wrfout = np.zeros(num_row)
        Pollu_obser['WRFOUT'] = init_wrfout
        #add iff urban column 
        Pollu_obser['IS_URBAN'] = init_wrfout

        #add value from wrfout to dataframe with matching location and date
        for index_obs,row_obs in Pollu_obser.iterrows():
            #print(row_obs)
            #get latitude and longitude
            curr_lat = row_obs['Latitude']
            curr_lon = row_obs['Longitude']
            #get i (for longitude) and j (for latitude) start from 1
            curr_i = Pollu_loc.loc[(Pollu_loc['Latitude']==curr_lat)&(Pollu_loc['Longitude']==curr_lon),'I'].tolist()[0]
            curr_j = Pollu_loc.loc[(Pollu_loc['Latitude']==curr_lat)&(Pollu_loc['Longitude']==curr_lon),'J'].tolist()[0]
            if curr_i >= 1000 or curr_j >= 1000:
                continue
            curr_i -= 0
            curr_j -= 0
            #get date
            curr_date = datetime.strptime(row_obs['Date Local'], '%Y-%m-%d')
            currday_inwrf = int(curr_date.strftime("%d"))-1
            curr_hour = datetime.strptime(row_obs['Time Local'], '%H:%M')
            currhour_inwrf = int(curr_hour.strftime("%H"))
            #get wrfout and write to dataframe
            try:
                curr_wrfout = Pollu_wrfout[currday_inwrf,currhour_inwrf,curr_j,curr_i].item()
                Pollu_obser['WRFOUT'][index_obs] = curr_wrfout
            except:
                Pollu_obser['WRFOUT'][index_obs] = 0.0
            #check if if urban type
            if lu_index[curr_j,curr_i] > 30:
                Pollu_obser['IS_URBAN'][index_obs] = 1
        
        # drop all rows with wrfout==0.0
        Pollu_obser = Pollu_obser.loc[Pollu_obser['WRFOUT']!=0]
        
        #write to excel
        Pollu_obser.rename(columns={'Sample Measurement':'Value'}, inplace=True)
        Pollu_obser.to_excel(compare_dir+'T2Hourly_2016_AQS_WRFOUT_'+months[currmonth][1]+'.xlsx')
        print(obser_dir+Pollu_obser_name+months[currmonth][1])

