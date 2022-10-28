'''
Extract timeline events for matches
Inputs include IdCompetition,IdSeason,IdStage,IdMatch
'''
import requests
import os
from pathlib import Path
import json
import sqlite3
import datetime
from datetime import timezone
from time import sleep

'''
Get detailed timeline of events in a match
'''
def get_timeline_for_match(IdCompetition,IdSeason,IdStage,IdMatch,output_date_folder):
    # https://api.fifa.com/api/v3/timelines/<IdCompetition>/<IdSeason>/<IdStage>/<IdMatch>?language=en
    url_match = 'https://api.fifa.com/api/v3/timelines/'+IdCompetition+'/'+IdSeason+'/'+IdStage+'/'+IdMatch+'?language=en'
    print(url_match)
    response_json = requests.get(url_match).json()

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
    
    # Update timeline metadata in db
    timeline_info_present = 'I' if not response_json['Event'] else 'Y'

    match_info = (  timeline_info_present,
                            datetime.datetime.now(timezone.utc).strftime(r"%Y-%m-%dT%H:%M:%S"),
                            response_json['IdCompetition'],
                            response_json['IdSeason'],
                            response_json['IdStage'],
                            # match_data['IdGroup'],
                            response_json['IdMatch'],
                            
                            
            )
    list_match_info = list()
    list_match_info.append(match_info)
    # print(process_date)
    
    db_update_timeline_flag(list_match_info=list_match_info,db_file_path=db_file_path)

'''
Update process_timeline flag for list of files that are processed
'''
def db_update_timeline_flag(list_match_info,db_file_path):
    try:
        sqliteConnection = sqlite3.connect(db_file_path)
        cursor = sqliteConnection.cursor()
        print(list_match_info)
        print("Connected to SQLite")

        sqlite_update_query = """UPDATE fifa_matches_log
                                    SET process_timeline = ?,
                                    ts_process_timeline = ?,
                                    count_process_timeline = count_process_timeline + 1
                                    WHERE  
                                    IdCompetition = ?
                                    AND IdSeason = ? 
                                    AND IdStage = ?
                                    AND IdMatch = ?;
                            """

        cursor.executemany(sqlite_update_query, list_match_info)
        sqliteConnection.commit()
        print("Total", cursor.rowcount, "Records updated successfully into fifa_matches_log table")
        sqliteConnection.commit()
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to UPDATE multiple records into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")

'''
Get list of matches from DB for a match_day
'''
def db_get_match_list(db_file_path,match_date):
    sqliteConnection = sqlite3.connect(db_file_path)
    cur = sqliteConnection.cursor()
    str_match_date = match_date.strftime('%Y-%m-%d')
    # str_match_date='2022-10-26'
    print("Connected to SQLite")
    
    # All events will be reprocessed 5 times
    # Event processing will start only 2 hours(7200 secs) after the match has started

    cur.execute("SELECT match_date, IdCompetition, IdSeason ,IdStage , IdMatch FROM fifa_matches_log \
                    where process_timeline in('Y','I','N') \
                    and count_process_timeline<=5    \
                    and strftime('%s')-CAST(strftime('%s', replace(match_date,'T',' ')) as integer)>7200 \
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

    # list_match_files = glob.glob(data_folder+"/*/match/*.json")
    # list_match_files.sort(reverse=True)


    # Initialize set to store list of dates that are processed
    set_str_match_date = set()
    
    # Define date ranges for processing
    base_date_diff = -1
    number_of_days_to_process = 5
    base = datetime.datetime.today()- datetime.timedelta(days=base_date_diff)
    date_list = [base - datetime.timedelta(days=x) for x in range(number_of_days_to_process)]


    for current_date in date_list:
    # For each match, get timeline events from API
        # Get list of matches for the day
        match_list = db_get_match_list(db_file_path,current_date)
        pass
        for match in match_list:
            str_match_date = match[0][:10].replace('-','_')
            
            match_date_folder = os.path.join(data_folder,str_match_date)
            get_timeline_for_match(match[1],match[2],match[3],match[4],match_date_folder)
