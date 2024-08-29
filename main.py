import os
import pandas as pd
import numpy as np
import utils

##### Multiprocessing test

if __name__ ==  '__main__':
    df_Compustat = pd.read_csv('Compustat for ODI.csv')
    df_ODI = pd.read_csv('ODI 2002-2011.csv')  # save with encoding (UTF-8) in sublime before using

    df_ODI['ESTAB_NAME'] = df_ODI['ESTAB_NAME'].str.lower()
    df_ODI['ESTAB_NAME2'] = df_ODI['ESTAB_NAME2'].str.lower()
    df_ODI_sorted = df_ODI.sort_values('ESTAB_NAME')

    df_Compustat['conm'] = df_Compustat['conm'].str.lower()
    df_Compustat_sorted = df_Compustat.sort_values('conm')

    matched_results = utils.parallel_fuzzy_matching(df_Compustat_sorted, 'conm', df_ODI_sorted['ESTAB_NAME'].tolist())
    df_Compustat_sorted['matched_NAME'] = matched_results





