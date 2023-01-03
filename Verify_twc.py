#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 13:43:04 2022

@author: chris matthews 

This is the main driver file that reads in information from the yaml file and then call specific functions from Verify_twc_lib.py
"""

import datetime as dt
import os
import pandas as pd
import sys
import Verify_twc_lib as ver_twc_lib


'''
Main driver file for calling Verify_twc_ lib functions
'''


if __name__ == "__main__":
    
    yaml_directory = '/directory/of/the/yaml/file'  
                 
    yaml_file = yaml_directory+'verify_twc_config.yml'
        
    # get the contexts of the file as a dictionary or throw an error if file doesn't exist
    try:
        params = ver_twc_lib.yaml_loader(yaml_file)    
    except FileNotFoundError:
        print('\nFile does not exist ', yaml_file)
        sys.exit('\nFile not found: '+ yaml_file)
    
    # get the string of id codes from the yml file
    id_string = params['id_code']
    # split them into a list
    id_code_list = id_string.strip("[]").split(",") 
    # create the dataframe
    df_plot = pd.DataFrame()
    
    for id_code in id_code_list:
        
        # get the combined dataset
        df_plot = ver_twc_lib.get_Combined_dataset(id_code, params)
        # plot the combined dataset 
        ver_twc_lib.plot_compare(df_plot,id_code, params)
        
        
     