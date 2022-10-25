'''
Extract post-match information: stadium info, match info such as winner,possession, tactics and player list
Inputs include IdCompetition,IdSeason,IdStage,IdMatch
'''
from cgitb import handler
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
Get team information for the match
'''
def get_teaminfo_for_match(IdCompetition,IdSeason,IdStage,IdMatch,output_date_folder):
    # https://api.fifa.com/api/v3/timelines/<IdCompetition>/<IdSeason>/<IdStage>/<IdMatch>?language=en
    url_match = 'https://api.fifa.com/api/v3/live/football/'+IdCompetition+'/'+IdSeason+'/'+IdStage+'/'+IdMatch+'?language=en'
    print(url_match)
    response_json = requests.get(url_match).json()

    teaminfo_date_folder = os.path.join(output_date_folder,'teaminfo')
    # Create directory if not exists: Suffix directory name
    mkdirpath = teaminfo_date_folder+'/'
    os.makedirs(mkdirpath, exist_ok=True)

    # Output file name
    output_file_name = os.path.join(teaminfo_date_folder,IdMatch+'.json')
    # Write match info to file
    with open(output_file_name, "w") as outfile:
        print(output_file_name)
        
        json.dump(response_json, outfile)
    # get_match_detail_for_match(IdCompetition,IdSeason,IdMatch,output_date_folder)

    # TODO: Build exception handler
    # requests.exceptions.ConnectionError: HTTPSConnectionPool(host='api.fifa.com', port=443): Max retries exceeded with url: /api/v3/live/football/2000000000/80foo89mm28qjvyhjzlpwj28k/80qbeanalyj5cvxikkq351iqc/7m64yiyuu2hjgep2kwim5so44?language=en (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x7fe5d5321b10>: Failed to establish a new connection: [Errno -3] Temporary failure in name resolution'))


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
        if match_date=='2022_10_24':

            sleep(2)
            match_file = open(match_file_name)
            match_data = json.load(match_file)
            print(match_date_folder)
            # get_timeline_for_match(IdCompetition,IdSeason,IdStage,IdMatch,output_date_folder)
            get_teaminfo_for_match(match_data['IdCompetition'],match_data['IdSeason'],match_data['IdStage'],match_data['IdMatch'],match_date_folder)
            pass

