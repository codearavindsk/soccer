'''
Extract timeline events for matches
Inputs include IdCompetition,IdSeason,IdStage,IdMatch
'''
import requests
import os
from pathlib import Path
import json
# import sqlite3
import datetime
import glob
import re
from time import sleep

'''
Get detailed timeline of events in a match
'''
def get_timeline_for_match(IdCompetition,IdSeason,IdStage,IdMatch,output_date_folder):
    # https://api.fifa.com/api/v3/timelines/<IdCompetition>/<IdSeason>/<IdStage>/<IdMatch>?language=en
    url_match = 'https://api.fifa.com/api/v3/timelines/'+IdCompetition+'/'+IdSeason+'/'+IdStage+'/'+IdMatch+'?language=en'
    print(url_match)
    response_json = requests.get(url_match).json()

    pass

    timeline_date_folder = os.path.join(output_date_folder,'timeline')
    # Create directory if not exists: Suffix directory name
    mkdirpath = timeline_date_folder+'/'
    os.makedirs(mkdirpath, exist_ok=True)

    # Output file name
    output_file_name = os.path.join(timeline_date_folder,IdMatch+'.json')
    # Write match info to file
    with open(output_file_name, "w") as outfile:
        # print(output_file_name)
        json.dump(response_json, outfile)

if __name__ == "__main__":

    current_dirname = os.path.dirname(__file__)
    # find one directory above current dir
    base_dir = Path(current_dirname).parents[0]

    data_folder = os.path.join(base_dir,'data')

    list_match_files = glob.glob(data_folder+"/*/match/*")
    list_match_files.sort(reverse=True)

    for match_file_name in list_match_files:
        match_date_folder = re.search(r"(.*\/\d{4}_\d{2}_\d{2})",match_file_name).group(1)
        match_date = re.search(r".*\/(\d{4}_\d{2}_\d{2})",match_file_name).group(1)

        # Cut of dates that are already processed
        # covered till 2022_09_17
        if match_date=='2022_10_23':

            sleep(2)
            match_file = open(match_file_name)
            match_data = json.load(match_file)
            print(match_date_folder)
            # get_timeline_for_match(IdCompetition,IdSeason,IdStage,IdMatch,output_date_folder)
            get_timeline_for_match(match_data['IdCompetition'],match_data['IdSeason'],match_data['IdStage'],match_data['IdMatch'],match_date_folder)
            pass

