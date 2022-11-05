import pandas as pd
import numpy as np
from pathlib import Path
import os
import glob
import re
from datetime import datetime
from google.cloud import storage

'''
Upload parquet files to cloud storage bucket
'''
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

'''
Load all JSON files in folder and flatten it to columnar structure
'''
def transform_files_in_directory(input_directory,output_directory,str_granularity_format):

    json_pattern = os.path.join(input_directory,'*.json')

    file_list = glob.glob(json_pattern)

    dfs = [] # an empty list to store the data frames
    for file in file_list:
        data = pd.read_json(file, lines=True) # read data frame from json file
        
        # Get match date from file path
        str_matchDate= re.search(r'.*(\d{4}_\d{2}_\d{2}).*',os.path.abspath(file)).group(1)#.replace("_","-")
        data['MatchDate'] = datetime.strptime(str_matchDate, '%Y_%m_%d')

        dfs.append(data) # append the data frame to the list

    df = pd.concat(dfs, ignore_index=True) # concatenate all the data frames in the list.
    print('DF after all files loaded:')
    print(df.shape)

    in_scope_competitions = [17,2000000000,2000001032]

    df1 = df.query('@in_scope_competitions in IdCompetition')
    df1.reset_index(drop=True, inplace=True)

    df1 = df1.convert_dtypes()

    df_awayTeam = pd.json_normalize(df1['Away']).drop(['PictureUrl','TeamName'], axis=1).add_prefix('AwayTeam_')#.explode(['TeamName'][0])
    df_homeTeam = pd.json_normalize(df1['Home']).drop(['PictureUrl','TeamName'], axis=1).add_prefix('HomeTeam_')#.explode(['TeamName'][0])

    df_StageNameNormalized = pd.json_normalize(df1['StageName'])
    df_StageName = pd.json_normalize(df_StageNameNormalized[0]).drop(['Locale'], axis=1).rename(columns={"Description": "StageNameDescription"})

    if not pd.json_normalize(df1['GroupName']).empty:
        df_GroupNameNormalized = pd.json_normalize(df1['GroupName'])
        df_GroupName = pd.json_normalize(df_GroupNameNormalized[0]).drop(['Locale'], axis=1).rename(columns={"Description": "GroupNameDescription"})
    else:
        df_GroupName = pd.DataFrame()
    
    df_CompetitionNameNormalized = pd.json_normalize(df1['CompetitionName'])
    df_CompetitionName = pd.json_normalize(df_CompetitionNameNormalized[0]).drop(['Locale'], axis=1).rename(columns={"Description": "CompetitionNameDescription"})

    df_Stadium = pd.json_normalize(df1['Stadium']).explode(['Name']).add_prefix('Stadium_')
    df_Stadium_Name = pd.json_normalize(df_Stadium['Stadium_Name']).drop(['Locale'], axis=1).rename(columns={"Description": "StadiumName"}).add_prefix('Stadium_')

    df_Stadium = pd.json_normalize(df1['Stadium']).explode(['CityName']).add_prefix('Stadium_')
    df_City_Name = pd.json_normalize(df_Stadium['Stadium_CityName']).drop(['Locale'], axis=1).rename(columns={"Description": "CityName"}).add_prefix('Stadium_')
    df_Stadium = df_Stadium.drop(['Stadium_Name','Stadium_CityName'], axis=1)


    df_match_info = df1.drop(['StageName','GroupName','CompetitionName','SeasonName','Home','Away','Stadium','BallPossession','Officials','Properties','IsUpdateable'], axis=1)

    # Match descriptions
    df_match_info = pd.merge(df_match_info, df_StageName, left_index=True, right_index=True)
    df_match_info = pd.merge(df_match_info, df_GroupName, how='left', left_index=True, right_index=True)
    df_match_info = pd.merge(df_match_info, df_CompetitionName, left_index=True, right_index=True)

    df_match_info = pd.merge(df_match_info, df_homeTeam, left_index=True, right_index=True)
    df_match_info = pd.merge(df_match_info, df_awayTeam, left_index=True, right_index=True)

    df_match_info = pd.merge(df_match_info, df_Stadium, left_index=True, right_index=True)
    df_match_info = pd.merge(df_match_info, df_Stadium_Name, left_index=True, right_index=True)
    df_match_info = pd.merge(df_match_info, df_City_Name, left_index=True, right_index=True)

    # df_match_info.info()

    df_match_info.drop(['Stadium_Properties.IdIFES'],axis=1,inplace=True)

    df_cleansed_match_info = df_match_info.replace({np.nan: None})
    df_cleansed_match_info['ts_processed_to_db'] = pd.Timestamp.utcnow()   
    df_cleansed_match_info.columns = map(str.lower, df_cleansed_match_info.columns)

    # List of columns that are object types
    columns_non_object_type = list(df_cleansed_match_info.select_dtypes(exclude=['object']).columns)

    # Convert object types to string
    df_non_object_cols = df_cleansed_match_info[columns_non_object_type]

    # List of columns that are object types
    columns_object_type = list(df_cleansed_match_info.select_dtypes(include=['object']).columns)


    # Convert object types to string
    df_object_cols = df_cleansed_match_info[columns_object_type].astype("string")
    # Build type corrected df
    df_type_corrected = pd.merge(df_non_object_cols, df_object_cols, left_index=True, right_index=True)

    print('Final DF before file write:')
    print(df.shape)
    
    # Set granularity for output files
    
    match_granular_list = df_type_corrected['matchdate'].dt.strftime(str_granularity_format).unique().tolist()

    # Create subfolder with granularity format
    output_directory = os.path.join(output_directory,str_granularity_format.replace('%',''))
    os.makedirs(os.path.dirname(output_directory+'/'), exist_ok=True)

    processed_file_list = list()
    for match_granular in match_granular_list:
        curr_filename = str(match_granular).replace('%','')+'_match.parquet.gzip'

        curr_filename_fullpath = os.path.join(output_directory,curr_filename)

        # Extract data from df based on granularity
        # Write df lines to parquet file
        df_type_corrected[df_type_corrected['matchdate'].dt.strftime(str_granularity_format)==match_granular].to_parquet(curr_filename_fullpath,
                compression='gzip')

        processed_file_list.append((curr_filename,curr_filename_fullpath))
    print('Files created: '+str(len(processed_file_list)))
    return(processed_file_list)


if __name__ == "__main__":

    current_dirname = os.path.dirname(__file__)
    # find one directory above current dirs
    base_dir = Path(current_dirname).parents[0]

    output_folder = os.path.join(base_dir,'dataNormalized','match')
    os.makedirs(os.path.dirname(output_folder+'/'), exist_ok=True)

    str_granularity_format = r"%y_%m"

    pattern_data_select = '2022_11_*'
    input_folder_pattern = os.path.join(base_dir,'data',pattern_data_select,'match')+'/'

    processed_file_list = transform_files_in_directory(input_directory=input_folder_pattern,
            output_directory=output_folder,
            str_granularity_format=str_granularity_format
            )

    # Load processed files to GCP
    for processed_file in processed_file_list:
        upload_blob(
        bucket_name='soccer-regional-asia',
        source_file_name=processed_file[1],
        destination_blob_name='match/'+processed_file[0],
    )