#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 23 17:03:47 2022

@author: chris matthews

This file is the library file (Verify_twc_lib.py) providing the functions used by Verify_twc.py

Namely extracting the information from the s3 buckets and possibly combining and graphing.
"""

import sys
import os
import json
import pandas as pd
import numpy as np
import datetime as dt
import yaml
import matplotlib.pyplot as plt
import seaborn as sns
import boto3
from sklearn.metrics import mean_squared_error
from math import sqrt
 
'''
Set up a yaml reader to read from a yml config file

'''
def yaml_loader(filepath):
    
    print('Using config file {}'.format(filepath))    
    
    with open(filepath,'r') as file:
            params = yaml.safe_load(file)    
            print(params)    
            return(params)    

'''
This functions gets the forecasts from the k51-forecasts s3 bucket. The required filename format is 
s3://k51-forecasts/twc/amazon/YYYY-MM-DD/YYYYMMDDTHH00/YYYYMMDDTHH00-HOURLY-sfc.csv.gz
The date is given in the yaml config file and is altered into the required format.

params:   params (from the parameters dictionary from the yaml file) - includes station code and initialization and forecast datetime plus the list 
          of parameters default is all 14. Same for number of hours in forecast - default is 24.
'''

def get_twc_Forecast(id_code, params):
    # get the time in the correct format for manipulation
    yaml_datetime_format = dt.datetime.strptime( params['init_time'],"%Y-%m-%d %H:%M")
    # get the code from the params file - though possible pass this in directly if a list of stations is given in the yaml file.
    id_yaml = id_code #params['id_code']

    BucketName = "k51-forecasts"  
    client = boto3.client("s3")
 
    num_days= params['num_days']
    num_hours= params['num_hours']  # set as 24 be default 
    
    fields_string = "temperature,temperatureDewPoint,temperatureFeelsLike,temperatureHeatIndex,relativeHumidity,windSpeed,windGust,windDirection,pressureMeanSeaLevel,pressureAltimeter,visibility,cloudCover,ceiling,uvIndex"
    
    df_forecast = pd.DataFrame  
    
    # initialise strings to hold the variables
    temp_index_list, id_code,init, temperature_fore,temperatureDewPoint_fore, temperatureFeelsLike_fore, temperatureHeatIndex_fore, \
    relativeHumidit_fore, windSpeed_fore, windGust_fore, windDirection_fore, pressureMeanSeaLevel_fore, pressureAltimeter_fore, \
    visibility_fore,ceiling_fore,uvIndex_fore =[],[],[],[],[], [], [], [], [],[], [], [], [],[] ,[],[]  

    date_for_directory = yaml_datetime_format
    dir_names = date_for_directory.strftime('%Y%m%dT%H%M')
    filename = date_for_directory.strftime('%Y%m%dT%H%M-HOURLY-sfc.csv.gz')
     
    #init time is 1 hour after filename title so is first forecast
    init_time =      (yaml_datetime_format+dt.timedelta(1/24)).strftime('%Y-%m-%dT%H:%M:%SZ') 
    forecast_time = yaml_datetime_format+dt.timedelta(1/24)
    
    # loop to get the forecasts for the number of days requested in yaml file.
    for i in range(num_days):
        top_dir_format = (yaml_datetime_format+dt.timedelta(1/24)).date()
        print('\nYYYY-MM-DD structure ', top_dir_format) 
                
        # need to add another day onto the directory
        #date_for_directory
        for j in range(num_hours):
            
            # forecast_time is the init_time incremented every hour + for each extra day - there are usually 360 hours of forecasts ??
            forecast_time = ((yaml_datetime_format+dt.timedelta(1/24)+dt.timedelta(i))+dt.timedelta(j/24)).strftime('%Y-%m-%dT%H:%M:%SZ')
            # get teh directory structure into the right format
            final = "twc/amazon/{}/{}/{}".format(top_dir_format,dir_names,filename)
            print(final)
            print(i,j,init_time,forecast_time)
            
            resp = client.select_object_content(
                Bucket=BucketName,
                Key=final, #'twc/amazon/2022-12-18/20221218T1900/20221218T1900-HOURLY-sfc.csv.gz',
                ExpressionType='SQL',                                                                                               
                Expression="""  SELECT id,init,fore, {} FROM s3object s where id=\'{}\' and init = '{}' and fore= '{}' """.format(fields_string,id_yaml,init_time,forecast_time) , 
                InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'GZIP'},
                OutputSerialization = {'CSV': {}
                               },
            )
            # decode the results of the query.
            data = [event['Records'] for event in resp['Payload'] if 'Records' in event   ]
            
            # read the values from the decoded data.
            for x in data:             # using replace here to get rid of new line \n characters and split to turn into a list
                results_string_forecast = x['Payload'].decode('utf-8').replace('\n','').split(",")
    
                # create a temporary index of the forecast time, that then allows comparison with the obs time when joined with df_observations.
                temp_index = dt.datetime.strptime(results_string_forecast[2],'%Y-%m-%dT%H:%M:%SZ')
                    
                temp_index_list.append(temp_index), 
                id_code.append(results_string_forecast[0]), 
                init.append(results_string_forecast[1]),                            
                temperature_fore.append(results_string_forecast[3]), 
                temperatureDewPoint_fore.append(results_string_forecast[4]),
                temperatureFeelsLike_fore.append(results_string_forecast[5]),
                temperatureHeatIndex_fore.append(results_string_forecast[6]),
                relativeHumidit_fore.append(results_string_forecast[7]),
                windSpeed_fore.append(results_string_forecast[8]),
                windGust_fore.append(results_string_forecast[9]),
                windDirection_fore.append(results_string_forecast[10]),
                pressureMeanSeaLevel_fore.append(results_string_forecast[11]),
                pressureAltimeter_fore.append(results_string_forecast[12]),
                visibility_fore.append(results_string_forecast[13]),
                            # no cloudCover in observations 'cloudCover_fore':results_string_forecast[14],
                ceiling_fore.append(results_string_forecast[15]),
                uvIndex_fore.append(results_string_forecast[16])
                           
                df_forecast = pd.DataFrame(zip(temp_index_list, id_code,init, temperature_fore,temperatureDewPoint_fore, temperatureFeelsLike_fore,
                                                   temperatureHeatIndex_fore, relativeHumidit_fore, windSpeed_fore, windGust_fore, windDirection_fore,
                                                   pressureMeanSeaLevel_fore, pressureAltimeter_fore, visibility_fore,ceiling_fore,uvIndex_fore),
                                                   columns=['temp_index', 'id_code','init', 'temperature_fore','temperatureDewPoint_fore', 'temperatureFeelsLike_fore',
                                                   'temperatureHeatIndex_fore', 'relativeHumidity_fore', 'windSpeed_fore', 'windGust_fore', 'windDirection_fore',
                                                   'pressureMeanSeaLevel_fore', 'pressureAltimeter_fore', 'visibility_fore','ceiling_fore','uvIndex_fore'])              
                
            # now set the temp index as the actual index
            df_forecast.index = df_forecast.temp_index
            df_forecast.index = pd.to_datetime(df_forecast.index)
            
    # finally convert to_numeric to allow for graphing etc - that are objects before this call
    df_forecast = df_forecast.apply(pd.to_numeric, errors='ignore')
    
    #finally drop the temp index column as it is now the index
    df_forecast.drop(['temp_index'],axis=1,inplace=True)
    return df_forecast

'''
This functions gets the observations from the k51-obs s3 bucket. The required filename format is 
s3://k51-obs/twc/amazon/YYYY-MM-DD/YYYYMMDDTHH00/YYYYMMDDTHH00-HOURLY-sfc.csv.gz
The date is given in the yaml config file and is altered into the required format.

params:   params 24 #(from the parameters dictionary from the yaml file) - includes station code and initialization datetime plus the list of parameters
         default is all 14. Same for number of hours in forecast - default is 24.
'''

def get_twc_Obs(id_code, params):
    
    # get the time in the correct format for manipulation
    yaml_datetime_format = dt.datetime.strptime( params['init_time'],"%Y-%m-%d %H:%M")
    # get the code from the params file - though possible pass this in directly if a list of stations is given in the yaml file.
    id_yaml = id_code  #params['id_code']
    
    #init time is 1 hour after filename title so is first forecast
    init_time =      (yaml_datetime_format+dt.timedelta(1/24)).strftime('%Y-%m-%dT%H:%M:%SZ') #timedate_for_directory.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    BucketName = "k51-obs"  
    client = boto3.client("s3")
    
    num_days= params['num_days']
    num_hours= params['num_hours']  # set as 24 be default ??
    
    # the fields extracted are now hardcoded to keep the structure correct - only difference with forecast fields is cloudCeiling and there is no cloudCover field
    fields_string = "temperature,temperatureDewPoint,temperatureFeelsLike,temperatureHeatIndex,relativeHumidity,windSpeed,windGust,windDirection,pressureMeanSeaLevel,pressureAltimeter,visibility,cloudCeiling,uvIndex"                        
   
    df_observations=pd.DataFrame()
    temp_index_list, id_code,init, temperature_obs,temperatureDewPoint_obs, temperatureFeelsLike_obs, temperatureHeatIndex_obs, \
    relativeHumidit_obs, windSpeed_obs, windGust_obs, windDirection_obs, pressureMeanSeaLevel_obs, pressureAltimeter_obs, \
    visibility_obs,ceiling_obs,uvIndex_obs =[],[],[],[],[], [], [], [], [],[], [], [], [],[] ,[],[]  

    for i in range(num_days):
        top_dir_format = (yaml_datetime_format+dt.timedelta(i)).date()
        
        # next need to check top_dir_fmat isn't ahead of now or else the code will crash
        if(top_dir_format >= dt.datetime.now().date()):
            print('\nOnly observations up to yesterday are available')
            break
        
        
        print('\nYYYY-MM-DD structure ', top_dir_format) #yaml_date_format+dt.timedelta(i)).date())
        for j in range(num_hours):
            #print(yaml_dateformat+dt.timedelta(i)+dt.timedelta(j/24))
            date_for_directory = yaml_datetime_format+dt.timedelta(i)+dt.timedelta(j/24)
            dir_names = date_for_directory.strftime('%Y%m%dT%H%M')
            filename = date_for_directory.strftime('%Y%m%dT%H%M-HOURLY-sfc.csv.gz')
            # forecast time is 1 hour after the time the file is written - hence adding an hour to the time
            observations_time = date_for_directory.strftime('%Y-%m-%dT%H:%M:%SZ')
            # get teh directory structure into the right format
            final = "twc/amazon/{}/{}/{}".format(top_dir_format,dir_names,filename)
            print('\nobs final:',final)
            print(observations_time)
            
            resp = client.select_object_content(
                                                Bucket=BucketName,
                                                Key= final,  #'twc/amazon/2022-12-18/20221218T2000/20221218T2000-HOURLY-sfc.csv.gz',
                                                ExpressionType='SQL',
                                                # since the obser vation time is not equal to the hour it was taken but always a 10-20minutes after need to use init> the date.
                                                Expression="""  SELECT id,init, {} FROM s3object s where id=\'{}\' and init > '{}'  """.format(fields_string,id_yaml,top_dir_format),
                                                InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'GZIP'},
                                                OutputSerialization = {'CSV': {}
                                               },
            )
    
            data = [event['Records'] for event in resp['Payload'] if 'Records' in event   ]
            #print(data)
            for x in data:
                results_string_observations = x['Payload'].decode('utf-8').replace('\n','').split(",")
              
                # create a temporary index of the forecast time, that then allows comparison with the obs time when joined with df_observations.
                temp_index = dt.datetime.strptime(results_string_observations[1],'%Y-%m-%d %H:%M:%S')
                # need to set the minutes and seconds =0 as the observation is on the hour but the arrival time is what is recorded in this field and that isn't exacly ti the hour as is reqiured
                temp_index = temp_index.replace(minute=0, second=0)
                #print('\n temp index', temp_index)
               
                temp_index_list.append(temp_index), 
                id_code.append(results_string_observations[0]), 
                init.append(results_string_observations[1]),                            
                temperature_obs.append(results_string_observations[2]), 
                temperatureDewPoint_obs.append(results_string_observations[3]),
                temperatureFeelsLike_obs.append(results_string_observations[4]),
                temperatureHeatIndex_obs.append(results_string_observations[5]),
                relativeHumidit_obs.append(results_string_observations[6]),
                windSpeed_obs.append(results_string_observations[7]),
                windGust_obs.append(results_string_observations[8]),
                windDirection_obs.append(results_string_observations[9]),
                pressureMeanSeaLevel_obs.append(results_string_observations[10]),
                pressureAltimeter_obs.append(results_string_observations[11]),
                visibility_obs.append(results_string_observations[12]),
                            # no cloudCover in observations 'cloudCover_obs':results_string_observations[14],
                ceiling_obs.append(results_string_observations[13]),
                uvIndex_obs.append(results_string_observations[14])
                           
                df_observations = pd.DataFrame(zip(temp_index_list, id_code,init, temperature_obs,temperatureDewPoint_obs, temperatureFeelsLike_obs,
                                                   temperatureHeatIndex_obs, relativeHumidit_obs, windSpeed_obs, windGust_obs, windDirection_obs,
                                                   pressureMeanSeaLevel_obs, pressureAltimeter_obs, visibility_obs,ceiling_obs,uvIndex_obs),
                                                   columns=['temp_index', 'id_code','init', 'temperature_obs','temperatureDewPoint_obs', 'temperatureFeelsLike_obs',
                                                   'temperatureHeatIndex_obs', 'relativeHumidity_obs', 'windSpeed_obs', 'windGust_obs', 'windDirection_obs',
                                                   'pressureMeanSeaLevel_obs', 'pressureAltimeter_obs', 'visibility_obs','ceiling_obs','uvIndex_obs'])
                                           
            # now set the temp index as the actual index            
            df_observations.index = df_observations.temp_index
            df_observations.index = pd.to_datetime(df_observations.index)
            
    # finally convert to_numeric to allow for graphing etc - that are objects before this call
    df_observations = df_observations.apply(pd.to_numeric, errors='ignore')
    
    #finally drop the temp index column as it is now the index
    df_observations.drop(['temp_index'],axis=1,inplace=True)
    
    return df_observations

'''
Function to generate plots of the data

params dataset, params
output is plot saved as .pdf#timedate_for_directory.strftime('%Y-%m-%dT%H:%M:%SZ')
'''

def plot_compare(dataset,id_code, params): 
    # set the figure size
    sns.set(rc = {'figure.figsize':(20,10)})
    # create a new figure for each plot 
    #plt.figure()
    
    
    # if the datasety hs forecast and observed temperatures then plot them.
    if('temperature_fore' in dataset and 'temperature_obs' in dataset):
        # create a new figure for each plot 
        plt.figure()
        # Calculate the root mean square error - use nanmean to avoid nan problems - and round to 3 decimal places
        RMSE = round(sqrt(np.nanmean(dataset.temperature_fore-dataset.temperature_obs)**2),3)
        print('\nRMSE is ', RMSE)
        
        sns.lineplot(data= dataset, y='temperature_fore', x=dataset.index, label='Forecast', marker='o')
        sns.lineplot(data= dataset, y='temperature_obs', x=dataset.index, label='Observed', marker='o')
        plt.legend(fontsize=10) #,loc='center left', bbox_to_anchor=(1, 0.5)) # sets the legend outside the plot to make readable
        plt.ylabel('degrees C')
        plt.xlabel('Date/time')
        plt.title('Forecast vs Observed Temperature for location {} for {} days from {} giving RMSE {} '.format(id_code, params['num_days'], params['init_time'],RMSE))
        plt.savefig(params['plots_dir']+id_code+'_temperature_'+params['init_time'][0:10]+'.pdf', bbox_inches = 'tight') 
        plt.close()
        print('\nfile saved '+params['plots_dir']+id_code+'_temperature_'+params['init_time'][0:10]+'.pdf')  
        
    # if the dataset has forecast and observed dewpoint temperatures then plot them.
    if('temperatureDewPoint_fore' in dataset and 'temperatureDewPoint_obs' in dataset):
        # create a new figure for each plot 
        plt.figure()
        # Calculate the root mean square error - use nanmean to avoid nan problems - and round to 3 decimal places
        RMSE = round(sqrt(np.nanmean(dataset.temperatureDewPoint_fore-dataset.temperatureDewPoint_obs)**2),3)
        print('\nRMSE is ', RMSE)
        
        sns.lineplot(data= dataset, y='temperatureDewPoint_fore', x=dataset.index, label='Forecast', marker='o')
        sns.lineplot(data= dataset, y='temperatureDewPoint_obs', x=dataset.index, label='Observed', marker='o')
        plt.legend(fontsize=10) #,loc='center left', bbox_to_anchor=(1, 0.5)) # sets the legend outside the plot to make readable
        plt.ylabel('degrees C')
        plt.xlabel('Date/time')
        plt.title('Forecast vs Observed Dewpoint Temperature for location {} for {} days from {} giving RMSE {} '.format(id_code, params['num_days'], params['init_time'],RMSE))
        plt.savefig(params['plots_dir']+id_code+'_temperatureDewPoint_'+params['init_time'][0:10]+'.pdf', bbox_inches = 'tight') 
        plt.close()
        print('\nfile saved '+params['plots_dir']+id_code+'_temperatureDewPoint_'+params['init_time'][0:10]+'.pdf')  
    
    # if the dataset has forecast and observed temperatureFeelsLike then plot them.
    if('temperatureFeelsLike_fore' in dataset and 'temperatureFeelsLike_obs' in dataset):
        # create a new figure for each plot 
        plt.figure()
        # Calculate the root mean square error - use nanmean to avoid nan problems - and round to 3 decimal places
        RMSE = round(sqrt(np.nanmean(dataset.temperatureFeelsLike_fore-dataset.temperatureFeelsLike_obs)**2),3)
        print('\nRMSE is ', RMSE)
        
        sns.lineplot(data= dataset, y='temperatureFeelsLike_fore', x=dataset.index, label='Forecast', marker='o')
        sns.lineplot(data= dataset, y='temperatureFeelsLike_obs', x=dataset.index, label='Observed', marker='o')
        plt.legend(fontsize=10) #,loc='center left', bbox_to_anchor=(1, 0.5)) # sets the legend outside the plot to make readable
        plt.ylabel('degrees C')
        plt.xlabel('Date/time')
        plt.title('Forecast vs Observed Feels like Temperature for location {} for {} days from {} giving RMSE {} '.format(id_code, params['num_days'], params['init_time'],RMSE))
        plt.savefig(params['plots_dir']+id_code+'_temperatureFeelsLike_'+params['init_time'][0:10]+'.pdf', bbox_inches = 'tight') 
        plt.close()
        print('\nfile saved '+params['plots_dir']+id_code+'_temperatureFeelsLike_'+params['init_time'][0:10]+'.pdf')  
        
    # if the dataset has forecast and observed relativehumidity then plot them.
    if('relativeHumidity_fore' in dataset and 'relativeHumidity_obs' in dataset):
        # create a new figure for each plot 
        plt.figure()
        # Calculate the root mean square error - use nanmean to avoid nan problems - and round to 3 decimal places
        RMSE = round(sqrt(np.nanmean(dataset.relativeHumidity_fore-dataset.relativeHumidity_obs)**2),3)
        print('\nRMSE is ', RMSE)
        
        sns.lineplot(data= dataset, y='relativeHumidity_fore', x=dataset.index, label='Forecast', marker='o')
        sns.lineplot(data= dataset, y='relativeHumidity_obs', x=dataset.index, label='Observed', marker='o')
        plt.legend(fontsize=10) #,loc='center left', bbox_to_anchor=(1, 0.5)) # sets the legend outside the plot to make readable
        plt.ylabel('Percentage')
        plt.xlabel('Date/time')
        plt.title('Forecast vs Observed RelativeHumidity for location {} for {} days from {} giving RMSE {} '.format(id_code, params['num_days'], params['init_time'],RMSE))
        plt.savefig(params['plots_dir']+id_code+'_relativeHumidity_'+params['init_time'][0:10]+'.pdf', bbox_inches = 'tight') 
        plt.close()
        print('\nfile saved '+params['plots_dir']+id_code+'_relativeHumidity_'+params['init_time'][0:10]+'.pdf')  
    
    # if the dataset has forecast and observed windSpeed then plot them.
    if('windSpeed_fore' in dataset and 'windSpeed_obs' in dataset):
        # create a new figure for each plot 
        plt.figure()
        # Calculate the root mean square error - use nanmean to avoid nan problems - and round to 3 decimal places
        RMSE = round(sqrt(np.nanmean(dataset.windSpeed_fore-dataset.windSpeed_obs)**2),3)
        print('\nRMSE is ', RMSE)
        
        sns.lineplot(data= dataset, y='windSpeed_fore', x=dataset.index, label='Forecast', marker='o')
        sns.lineplot(data= dataset, y='windSpeed_obs', x=dataset.index, label='Observed', marker='o')
        plt.legend(fontsize=10) #,loc='center left', bbox_to_anchor=(1, 0.5)) # sets the legend outside the plot to make readable
        plt.ylabel('m/s or km/h ??')
        plt.xlabel('Date/time')
        plt.title('Forecast vs Observed Windspeed for location {} for {} days from {} giving RMSE {} '.format(id_code, params['num_days'], params['init_time'],RMSE))
        plt.savefig(params['plots_dir']+id_code+'_windSpeed_'+params['init_time'][0:10]+'.pdf', bbox_inches = 'tight') 
        plt.close()
        print('\nfile saved '+params['plots_dir']+id_code+'_windSpeed_'+params['init_time'][0:10]+'.pdf')  
       
    # if the dataset has forecast and observed winddirection then plot them.
    if('windDirection_fore' in dataset and 'windDirection_obs' in dataset):
        # create a new figure for each plot 
        plt.figure()
        # Calculate the root mean square error - use nanmean to avoid nan problems - and round to 3 decimal places
        RMSE = round(sqrt(np.nanmean(dataset.windDirection_fore-dataset.windDirection_obs)**2),3)
        print('\nRMSE is ', RMSE)
        
        sns.lineplot(data= dataset, y='windDirection_fore', x=dataset.index, label='Forecast', marker='o')
        sns.lineplot(data= dataset, y='windDirection_obs', x=dataset.index, label='Observed', marker='o')
        plt.legend(fontsize=10) #,loc='center left', bbox_to_anchor=(1, 0.5)) # sets the legend outside the plot to make readable
        plt.ylabel('degrees')
        plt.xlabel('Date/time')
        plt.title('Forecast vs Observed Wind Direction for location {} for {} days from {} giving RMSE {} '.format(id_code, params['num_days'], params['init_time'],RMSE))
        plt.savefig(params['plots_dir']+id_code+'_windDirection_'+params['init_time'][0:10]+'.pdf', bbox_inches = 'tight') 
        plt.close()
        print('\nfile saved '+params['plots_dir']+id_code+'_windDirection_'+params['init_time'][0:10]+'.pdf')  
      
    # if the dataset has forecast and observed pressureMeanSeaLevel then plot them.
    if('pressureMeanSeaLevel_fore' in dataset and 'pressureMeanSeaLevel_obs' in dataset):
        # create a new figure for each plot 
        plt.figure()
        # Calculate the root mean square error - use nanmean to avoid nan problems - and round to 3 decimal places
        RMSE = round(sqrt(np.nanmean(dataset.pressureMeanSeaLevel_fore-dataset.pressureMeanSeaLevel_obs)**2),3)
        print('\nRMSE is ', RMSE)
        
        sns.lineplot(data= dataset, y='pressureMeanSeaLevel_fore', x=dataset.index, label='Forecast', marker='o')
        sns.lineplot(data= dataset, y='pressureMeanSeaLevel_obs', x=dataset.index, label='Observed', marker='o')
        plt.legend(fontsize=10) #,loc='center left', bbox_to_anchor=(1, 0.5)) # sets the legend outside the plot to make readable
        plt.ylabel('hPa')
        plt.xlabel('Date/time')
        plt.title('Forecast vs Observed pressureMeanSeaLevel for location {} for {} days from {} giving RMSE {} '.format(id_code, params['num_days'], params['init_time'],RMSE))
        plt.savefig(params['plots_dir']+id_code+'_pressureMeanSeaLevel_'+params['init_time'][0:10]+'.pdf', bbox_inches = 'tight') 
        plt.close()
        print('\nfile saved '+params['plots_dir']+id_code+'_pressureMeanSeaLevel_'+params['init_time'][0:10]+'.pdf')  
        
    # if the dataset has forecast and observed cloudCover then plot them.
    if('ceiling_fore' in dataset and 'cloudCeiling_obs' in dataset):
        # create a new figure for each plot 
        plt.figure()
        # Calculate the root mean square error - use nanmean to avoid nan problems - and round to 3 decimal places
        RMSE = round(sqrt(np.nanmean(dataset.ceiling_fore-dataset.cloudCeiling_obs)**2),3)
        print('\nRMSE is ', RMSE)
        
        sns.lineplot(data= dataset, y='ceiling_fore', x=dataset.index, label='Forecast', marker='o')
        sns.lineplot(data= dataset, y='cloudCeiling_obs', x=dataset.index, label='Observed', marker='o')
        plt.legend(fontsize=10) 
        plt.ylabel('height in metres ??')
        plt.xlabel('Date/time')
        plt.title('Forecast vs Observed cloudCeiling for location {} for {} days from {} giving RMSE {} '.format(id_code, params['num_days'], params['init_time'],RMSE))
        plt.savefig(params['plots_dir']+id_code+'_cloudCeiling_'+params['init_time'][0:10]+'.pdf', bbox_inches = 'tight') 
        plt.close()
        print('\nfile saved '+params['plots_dir']+id_code+'_cloudCeiling_'+params['init_time'][0:10]+'.pdf')  
        
'''
Main driving program that calls each of the functions and joins the dataframes from each section based on the flags passed to it

params params dictionary from yaml

returns the combined dataset
'''

def get_Combined_dataset(id_code,params): # , field
    # define empty dataframes
    df_forecasts,df_observations, df_combined = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    
    
    
    # get the observations 
    if (params['forecasts_flag']):
        try:
            df_forecasts = get_twc_Forecast(id_code, params)
            # if we only want to plot the forecast set df_combined = df_forecast
            df_combined = df_forecasts
        except:
            print('\nThere was a problem getting the forecasts')
            
    # get the forecasts    
    if(params['observations_flag']):
        try:
            df_observations = get_twc_Obs(id_code, params)
            # if we only want to plot the observations set df_combined = df_observations
            df_combined = df_observations
        except:
            print('\nThere was a problem getting the observations')
            
    if(params['combined_flag']):
        
        try:
            df_forecasts = get_twc_Forecast(id_code, params)            
        except:
            print('\nThere was a problem getting the forecasts')
            
        try:
            df_observations = get_twc_Obs(id_code, params)
        except:
            print('\nThere was a problem getting the observations')
            
        try:    
            df_combined = df_forecasts.merge(df_observations, on='temp_index', how='outer')
        except:
            print('\nThere was a problem merging the observations and the forecasts')
    #print(df_combined)
    return df_combined    

