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
    

    in_scope_competitions = [17,2000000000,2000001032]

    df_match_info = df.query('@in_scope_competitions in IdCompetition')
    df_match_info.reset_index(drop=True, inplace=True)

    df_awayPlayers = df_match_info.join(pd.json_normalize(df_match_info.AwayTeam).add_prefix('AwayTeam_')).drop(columns=['AwayTeam'])
    df_explodeAwayPlayers = df_awayPlayers.explode('AwayTeam_Players',ignore_index=True)
    df_AwayPlayersNormalized = df_explodeAwayPlayers.join(pd.json_normalize(df_explodeAwayPlayers.AwayTeam_Players).add_prefix('AwayTeamPlayer_'))

    # Get first element of list: Player name will always be a list of size 1
    df_AwayPlayersNormalized["AwayTeamPlayer_PlayerName_Expand"] = df_AwayPlayersNormalized["AwayTeamPlayer_PlayerName"].str[0]
    # Remove column from source df
    df_AwayPlayersNormalized.drop(columns =['AwayTeamPlayer_PlayerName'], inplace=True)
    df_AwayPlayersNormalized = df_AwayPlayersNormalized.join(pd.json_normalize(df_AwayPlayersNormalized['AwayTeamPlayer_PlayerName_Expand']).add_prefix('AwayTeamPlayer_').drop(columns=['AwayTeamPlayer_Locale'])).rename(columns={"AwayTeamPlayer_Description": "AwayTeamPlayer_PlayerName"})
    # Remove temporary column from source df
    df_AwayPlayersNormalized.drop(columns =['AwayTeamPlayer_PlayerName_Expand'], inplace=True)

    df_AwayPlayersNormalized["AwayTeamPlayer_ShortName_Expand"] = df_AwayPlayersNormalized["AwayTeamPlayer_ShortName"].str[0]
    # Remove column from source df
    df_AwayPlayersNormalized.drop(columns =['AwayTeamPlayer_ShortName'], inplace=True)
    df_AwayPlayersNormalized = df_AwayPlayersNormalized.join(pd.json_normalize(df_AwayPlayersNormalized['AwayTeamPlayer_ShortName_Expand']).add_prefix('AwayTeamPlayer_').drop(columns=['AwayTeamPlayer_Locale'])).rename(columns={"AwayTeamPlayer_Description": "AwayTeamPlayer_ShortName"})

    # Remove temporary column from source df
    df_AwayPlayersNormalized.drop(columns =['AwayTeamPlayer_ShortName_Expand'], inplace=True)

    df_AwayPlayersNormalized["AwayTeam_TeamName_Expand"] = df_AwayPlayersNormalized["AwayTeam_TeamName"].str[0]
    # Remove column from source df
    # df_AwayPlayersNormalized.drop(columns =['AwayTeam_TeamName'], inplace=True)
    df_AwayPlayersNormalized = df_AwayPlayersNormalized.join(pd.json_normalize(df_AwayPlayersNormalized['AwayTeam_TeamName_Expand']).add_prefix('AwayTeamName_').drop(columns=['AwayTeamName_Locale'])).rename(columns={"AwayTeamName_Description": "TeamName_Description"})

    # # Remove temporary column from source df
    df_AwayPlayersNormalized.drop(columns =['AwayTeam_TeamName_Expand'], inplace=True)

    df_homePlayers = df_match_info.join(pd.json_normalize(df_match_info.HomeTeam).add_prefix('HomeTeam_')).drop(columns=['HomeTeam'])
    
    df_explodeHomePlayers = df_homePlayers.explode('HomeTeam_Players',ignore_index=True)
    df_HomePlayersNormalized = df_explodeHomePlayers.join(pd.json_normalize(df_explodeHomePlayers.HomeTeam_Players).add_prefix('HomeTeamPlayer_'))
    

    # Get first element of list: Player name will always be a list of size 1
    df_HomePlayersNormalized["HomeTeamPlayer_PlayerName_Expand"] = df_HomePlayersNormalized["HomeTeamPlayer_PlayerName"].str[0]
    # Remove column from source df
    df_HomePlayersNormalized.drop(columns =['HomeTeamPlayer_PlayerName'], inplace=True)
    df_HomePlayersNormalized = df_HomePlayersNormalized.join(pd.json_normalize(df_HomePlayersNormalized['HomeTeamPlayer_PlayerName_Expand']).add_prefix('HomeTeamPlayer_').drop(columns=['HomeTeamPlayer_Locale'])).rename(columns={"HomeTeamPlayer_Description": "HomeTeamPlayer_PlayerName"})
    # Remove temporary column from source df
    df_HomePlayersNormalized.drop(columns =['HomeTeamPlayer_PlayerName_Expand'], inplace=True)


    df_HomePlayersNormalized["HomeTeamPlayer_ShortName_Expand"] = df_HomePlayersNormalized["HomeTeamPlayer_ShortName"].str[0]
    # Remove column from source df
    df_HomePlayersNormalized.drop(columns =['HomeTeamPlayer_ShortName'], inplace=True)
    df_HomePlayersNormalized = df_HomePlayersNormalized.join(pd.json_normalize(df_HomePlayersNormalized['HomeTeamPlayer_ShortName_Expand']).add_prefix('HomeTeamPlayer_').drop(columns=['HomeTeamPlayer_Locale'])).rename(columns={"HomeTeamPlayer_Description": "HomeTeamPlayer_ShortName"})

    # Remove temporary column from source df
    df_HomePlayersNormalized.drop(columns =['HomeTeamPlayer_ShortName_Expand'], inplace=True)

    df_HomePlayersNormalized["HomeTeam_TeamName_Expand"] = df_HomePlayersNormalized["HomeTeam_TeamName"].str[0]
    # Remove column from source df
    # df_HomePlayersNormalized.drop(columns =['HomeTeam_TeamName'], inplace=True)
    df_HomePlayersNormalized = df_HomePlayersNormalized.join(pd.json_normalize(df_HomePlayersNormalized['HomeTeam_TeamName_Expand']).add_prefix('HomeTeamName_').drop(columns=['HomeTeamName_Locale'])).rename(columns={"HomeTeamName_Description": "TeamName_Description"})

    # # Remove temporary column from source df
    df_HomePlayersNormalized.drop(columns =['HomeTeam_TeamName_Expand'], inplace=True)
    
    df_HomePlayersNormalized['TeamStatus'] = 'HOME'
    df_AwayPlayersNormalized['TeamStatus'] = 'AWAY'

    df_match_info = df[['MatchDate','IdMatch','IdStage','IdGroup','IdSeason','IdCompetition','CompetitionName','SeasonName','Stadium','ResultType','Attendance','Date','LocalDate','FirstHalfExtraTime','SecondHalfExtraTime','Winner','BallPossession','HomeTeam','AwayTeam']].query('@in_scope_competitions in IdCompetition')
    # df_match_info = df.query('@in_scope_competitions in IdCompetition')
    df_match_info.reset_index(drop=True, inplace=True)

    df_match_info = df_match_info.join(pd.json_normalize(df_match_info['BallPossession']).add_prefix('BallPossession_'))
    df_match_info.drop(columns =['BallPossession'], inplace=True)

    df_match_info = df_match_info.join(pd.json_normalize(df_match_info['Stadium']).add_prefix('Stadium_'))
    df_match_info.drop(columns =['Stadium'], inplace=True)
    # df_match_info.info()

    df_match_info["Stadium_Name_Expand"] = df_match_info["Stadium_Name"].str[0]
    df_match_info["Stadium_Name_Expand"]

    # df_match_info.drop(columns =['Stadium_Name'], inplace=True)
    df_match_info = df_match_info.join(pd.json_normalize(df_match_info['Stadium_Name_Expand']).add_prefix('StadiumName_').drop(columns=['StadiumName_Locale']))#.rename(columns={"StadiumName_Description": "AwayTeamPlayer_PlayerName"})
    # Remove temporary column from source df
    df_match_info.drop(columns =['Stadium_Name_Expand'], inplace=True)
    df_match_info["Stadium_CityName_Expand"] = df_match_info["Stadium_CityName"].str[0]


    # df_match_info.drop(columns =['Stadium_Name'], inplace=True)
    df_match_info = df_match_info.join(pd.json_normalize(df_match_info['Stadium_CityName_Expand']).add_prefix('StadiumCityName_').drop(columns=['StadiumCityName_Locale']))#.rename(columns={"StadiumName_Description": "AwayTeamPlayer_PlayerName"})
    # Remove temporary column from source df
    df_match_info.drop(columns =['Stadium_CityName_Expand'], inplace=True)

    df_match_info = df_match_info.join(pd.json_normalize(df_match_info.AwayTeam).add_prefix('AwayTeam_')).drop(columns=['AwayTeam'])
    df_match_info = df_match_info.join(pd.json_normalize(df_match_info.HomeTeam).add_prefix('HomeTeam_')).drop(columns=['HomeTeam'])

    df_match_info.drop(columns =['AwayTeam_TeamName','AwayTeam_Coaches','AwayTeam_Players','AwayTeam_Bookings','AwayTeam_Goals','AwayTeam_Substitutions',
    'HomeTeam_TeamName','HomeTeam_Coaches','HomeTeam_Players','HomeTeam_Bookings','HomeTeam_Goals','HomeTeam_Substitutions',
    'CompetitionName','SeasonName'
    ],inplace=True)

    df_cleansed_match_info = df_match_info.replace({np.nan: None})
    df_cleansed_match_info['ts_processed_to_db'] = pd.Timestamp.utcnow()   
    df_cleansed_match_info.columns = map(str.lower, df_cleansed_match_info.columns)


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


    # Set granularity for output files
    
    match_granular_list = df_type_corrected['matchdate'].dt.strftime(str_granularity_format).unique().tolist()

    # Create subfolder with granularity format
    output_directory = os.path.join(output_directory,str_granularity_format.replace('%',''))
    os.makedirs(os.path.dirname(output_directory+'/'), exist_ok=True)

    processed_file_list = list()
    for match_granular in match_granular_list:
        curr_filename = str(match_granular).replace('%','')+'_match_post_info.parquet.gzip'

        curr_filename_fullpath = os.path.join(output_directory,curr_filename)

        # Extract data from df based on granularity
        # Write df lines to parquet file
        df_type_corrected[df_type_corrected['matchdate'].dt.strftime(str_granularity_format)==match_granular].to_parquet(curr_filename_fullpath,
                compression='gzip')

        processed_file_list.append((curr_filename,curr_filename_fullpath))
    
    return(processed_file_list)


if __name__ == "__main__":

    current_dirname = os.path.dirname(__file__)
    # find one directory above current dirfas
    base_dir = Path(current_dirname).parents[0]

    output_folder = os.path.join(base_dir,'dataNormalized','match_post_info')
    os.makedirs(os.path.dirname(output_folder+'/'), exist_ok=True)

    str_granularity_format = r"%y_%m"

    pattern_data_select = '2022_*_*'
    input_folder_pattern = os.path.join(base_dir,'data',pattern_data_select,'match')+'/'
    
    processed_file_list = transform_files_in_directory(input_directory=input_folder_pattern,output_directory=output_folder,str_granularity_format=str_granularity_format)
    pass

    for processed_file in processed_file_list:
        upload_blob(
        bucket_name='soccer-regional-asia',
        source_file_name=processed_file[1],
        destination_blob_name='match_post_info/'+processed_file[0],
    )