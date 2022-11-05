import pandas as pd
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
    
    # Exlcude files that do not have Event information
    df = df[df['Event'].apply(len)>0]

    in_scope_competitions = [17,2000000000,2000001032]

    df_match_info = df.query('@in_scope_competitions in IdCompetition')
    df_match_info.reset_index(drop=True, inplace=True)
    
    df_explodeEvent = df_match_info.explode('Event',ignore_index=True)
    df_eventNormalized = df_explodeEvent.join(pd.json_normalize(df_explodeEvent.Event).add_prefix('Event_'))

    # Get event description
    df_eventNormalized["Event_EventDescription_Expand"] = df_eventNormalized["Event_EventDescription"].str[0]
    df_eventNormalized.drop(columns =['Event_EventDescription'], inplace=True)
    df_eventNormalized = df_eventNormalized.join(pd.json_normalize(df_eventNormalized['Event_EventDescription_Expand']).add_prefix('Event_EventDescription_').drop(columns=['Event_EventDescription_Locale']))#.rename(columns={"Event_TypeLocalized_Description": "AwayTeamPlayer_PlayerName"})
    # Remove temporary column from source df
    df_eventNormalized.drop(columns =['Event_EventDescription_Expand'], inplace=True)

    # Get event type localized
    df_eventNormalized["Event_TypeLocalized_Expand"] = df_eventNormalized["Event_TypeLocalized"].str[0]
    df_eventNormalized.drop(columns =['Event_TypeLocalized'], inplace=True)
    df_eventNormalized = df_eventNormalized.join(pd.json_normalize(df_eventNormalized['Event_TypeLocalized_Expand']).add_prefix('Event_TypeLocalized_').drop(columns=['Event_TypeLocalized_Locale']))#.rename(columns={"Event_TypeLocalized_Description": "AwayTeamPlayer_PlayerName"})
    # Remove temporary column from source df
    df_eventNormalized.drop(columns =['Event_TypeLocalized_Expand'], inplace=True)

    # Remove unwanted columns
    df_eventNormalized.drop(columns =['Event','Properties','Event_Qualifiers'], inplace=True)


    # Setup dataframe for storage
    # Convert objects/dicts to strings

    # List of columns that are object types
    columns_non_object_type = list(df_eventNormalized.select_dtypes(exclude=['object']).columns)
    columns_non_object_type

    # Convert object types to string
    df_non_object_cols = df_eventNormalized[columns_non_object_type]
    df_non_object_cols.shape

    # List of columns that are object types
    columns_object_type = list(df_eventNormalized.select_dtypes(include=['object']).columns)
    columns_object_type

    # Convert object types to string
    df_object_cols = df_eventNormalized[columns_object_type].astype("string")
    df_object_cols.shape

    df_type_corrected = pd.merge(df_non_object_cols, df_object_cols, left_index=True, right_index=True)
    df_type_corrected.shape

    # Convert timestamp field from string to datetime
    df_type_corrected['Event_Timestamp'] = pd.to_datetime(df_type_corrected['Event_Timestamp'], format='%Y-%m-%dT%H:%M:%S')

    # Make DF column names headers to lower case
    df_type_corrected.columns = map(str.lower, df_type_corrected.columns)
    
    # Set granularity for output files
    
    match_granular_list = df_type_corrected['matchdate'].dt.strftime(str_granularity_format).unique().tolist()

    # Create subfolder with granularity format
    output_directory = os.path.join(output_directory,str_granularity_format.replace('%',''))
    os.makedirs(os.path.dirname(output_directory+'/'), exist_ok=True)

    processed_file_list = list()
    for match_granular in match_granular_list:
        curr_filename = str(match_granular).replace('%','')+'_timeline.parquet.gzip'

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

    output_folder = os.path.join(base_dir,'dataNormalized','timeline')
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
        destination_blob_name='match_timeline/'+processed_file[0],
    )