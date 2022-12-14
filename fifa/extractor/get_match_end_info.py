'''
Extract post-match information: stadium info, match info such as winner,possession, tactics and player list
Inputs include IdCompetition,IdSeason,IdStage,IdMatch
'''
from cgitb import handler
import requests
import os
from pathlib import Path
import json
import sqlite3
import datetime
import glob
import re
from time import sleep
from datetime import timezone

'''
Get team information for the match
'''
def get_teaminfo_for_match(IdCompetition,IdSeason,IdStage,IdMatch,output_date_folder):
    # https://api.fifa.com/api/v3/timelines/<IdCompetition>/<IdSeason>/<IdStage>/<IdMatch>?language=en
    url_match = 'https://api.fifa.com/api/v3/live/football/'+IdCompetition+'/'+IdSeason+'/'+IdStage+'/'+IdMatch+'?language=en'
    response_json = requests.get(url_match).json()
    # TODO: Handle HTTPSConnectionPool(host='api.fifa.com', port=443): Max retries exceeded with url:

    teaminfo_date_folder = os.path.join(output_date_folder,'teaminfo')
    # Create directory if not exists: Suffix directory name
    mkdirpath = teaminfo_date_folder+'/'
    os.makedirs(mkdirpath, exist_ok=True)

    # Output file name
    output_file_name = os.path.join(teaminfo_date_folder,IdMatch+'.json')

    # Write match info to file
    with open(output_file_name, "w") as outfile:
        json.dump(response_json, outfile)
    
    # if no response received, exit the function
    if not response_json:
        return

    # Update DB with match info status
    # I, incomplete if Event information not in file
    # Y, if file has Event information
    # timeline_info_present = 'I' if not match_data['Event'] else 'Y'
    team_info_present = 'Y'

    match_info = (  team_info_present,
                    datetime.datetime.now(timezone.utc).strftime(r"%Y-%m-%dT%H:%M:%S"),
                    response_json['IdCompetition'],
                    response_json['IdSeason'],
                    response_json['IdStage'],
                    # match_data['IdGroup'],
                    response_json['IdMatch'],
                    
                    
        )
    list_match_info = list()
    list_match_info.append(match_info)
    
    
    db_update_match_end_info_flag(match_info=match_info,db_file_path=db_file_path)
    # get_match_detail_for_match(IdCompetition,IdSeason,IdMatch,output_date_folder)

    # TODO: Build exception handler
    # requests.exceptions.ConnectionError: HTTPSConnectionPool(host='api.fifa.com', port=443): Max retries exceeded with url: /api/v3/live/football/2000000000/80foo89mm28qjvyhjzlpwj28k/80qbeanalyj5cvxikkq351iqc/7m64yiyuu2hjgep2kwim5so44?language=en (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x7fe5d5321b10>: Failed to establish a new connection: [Errno -3] Temporary failure in name resolution'))

'''
Update process_match_end_info flag for list of files that are processed
'''
def db_update_match_end_info_flag(match_info,db_file_path):
    try:
        sqliteConnection = sqlite3.connect(db_file_path)
        cursor = sqliteConnection.cursor()
        
        sqlite_update_query = """UPDATE fifa_matches_log
                                    SET process_match_end_info = ?,
                                    ts_process_match_end_info = ?,
                                    count_process_match_end_info = count_process_match_end_info + 1
                                    WHERE  
                                    IdCompetition = ?
                                    AND IdSeason = ? 
                                    AND IdStage = ?
                                    AND IdMatch = ?;
                            """

        cursor.execute(sqlite_update_query, match_info)
        sqliteConnection.commit()
        sqliteConnection.commit()
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to UPDATE multiple records into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()

'''
Get list of matches from DB for a match_day
'''
def db_get_match_list(db_file_path,match_date):
    sqliteConnection = sqlite3.connect(db_file_path)
    cur = sqliteConnection.cursor()
    str_match_date = match_date.strftime('%Y-%m-%d')
# and count_process_match_end_info<=5 \
    # Events that started at least 2 hours(7200 secs) before script run time
    cur.execute("SELECT match_date, IdCompetition, IdSeason ,IdStage , IdMatch FROM fifa_matches_log \
                    where process_match_end_info in('N','I','Y') \
                    and strftime('%s')-CAST(strftime('%s', replace(match_date,'T',' ')) as integer)>7200 \
                    and IdCompetition in ('2000000000','2000001032','17') \
                    AND date(match_date)= (?)",[str_match_date])
    match_list = cur.fetchall()
    cur.close()
    return match_list

if __name__ == "__main__":

    # define path to db, created in get_calendar_match
    db_file_path = os.path.join('db','fifaProcessDB.db')

    current_dirname = os.path.dirname(__file__)
    # find one directory above current dir
    base_dir = Path(current_dirname).parents[0]

    data_folder = os.path.join(base_dir,'data')

    # Initialize set to store list of dates that are processed
    set_str_match_date = set()
    
    # Define date ranges for processing
    # done fof 365 * 2
    base_date_diff = 0
    number_of_days_to_process = 7
    base = datetime.datetime.today()- datetime.timedelta(days=base_date_diff)
    date_list = [base - datetime.timedelta(days=x) for x in range(number_of_days_to_process)]


    for current_date in date_list:
        print(current_date)

        # Get list of matches for the day
        match_list = db_get_match_list(db_file_path,current_date)
        for match in match_list:
            str_match_date = match[0][:10].replace('-','_')
            
            match_date_folder = os.path.join(data_folder,str_match_date)
            get_teaminfo_for_match(match[1],match[2],match[3],match[4],match_date_folder)
