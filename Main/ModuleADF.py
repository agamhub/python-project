import os
import pandas as pd

directory = "/home/user/python-project/File/ADF/"
files = os.listdir(directory)
df_all = None

def adf_function():
    for file in files:
        df = pd.read_json(directory+file)
        df = df['properties']['activities']    
        names = [activity['name'] for activity in df]
        df_final = pd.DataFrame({'ADF Name': file[:-5],'Activity Names': names})
        df_final['Application'] = df_final['Activity Names'].str.split('_').str[0]
        df_final['TableName'] = df_final['Activity Names'].str.split('_').str[2:].str.join('_')
        df_final['FinalTable'] = 'STAG_' + df_final['Application'] + '_' + df_final['TableName'] 
        df_final = df_final[['ADF Name', 'FinalTable', 'Activity Names']]

        if 'df_all' in locals():
            df_all = pd.concat([df_all, df_final], ignore_index=True)
        else:
            df_all = df_final
    return df_all