'''
Extract list of matches from the calendar
Inputs include 'from' and 'to' dates. Alsoinclude the count of records to be pulled
'''
import requests
import os
from pathlib import Path
import json
import sqlite3
import datetime
from datetime import timezone
import pandas as pd
from glob import glob


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

'''
Get match details. Possible overlap with calendar match info.
Includes match day info such as attendance, weather, team formation, possession, officials
'''
def get_match_detail_for_match(IdCompetition,IdSeason,IdStage,IdMatch,output_date_folder):
    # https://api.fifa.com/api/v3/timelines/<IdCompetition>/<IdSeason>/<IdStage>/<IdMatch>?language=en
    url_match = 'https://api.fifa.com/api/v3/calendar/'+IdCompetition+'/'+IdSeason+'/'+IdMatch+'?language=en'
    print(url_match)
    response_json = requests.get(url_match).json()

    pass

    match_detail_date_folder = os.path.join(output_date_folder,'match_detail')
    # Create directory if not exists: Suffix directory name
    mkdirpath = match_detail_date_folder+'/'
    os.makedirs(mkdirpath, exist_ok=True)

    # Output file name
    output_file_name = os.path.join(match_detail_date_folder,IdMatch+'.json')
    # Write match info to file
    with open(output_file_name, "w") as outfile:
        print(output_file_name)
        
        json.dump(response_json, outfile)
    
    get_teaminfo_for_match(IdCompetition,IdSeason,IdStage,IdMatch,output_date_folder)


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
        print(output_file_name)
        
        json.dump(response_json, outfile)
    get_match_detail_for_match(IdCompetition,IdSeason,IdStage,IdMatch,output_date_folder)


'''
Get details of matches based on URL parameters
'''
def get_calendar_matches(url_params,output_folder):
    headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:104.0) Gecko/20100101 Firefox/104.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    #   'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cache-Control': 'no-store',
    'X-Requested-With': 'XMLHttpRequest',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'DNT': '1',
    'Sec-GPC': '1',
    'Connection': 'keep-alive'
    }
    
    try:
        base_url = 'https://api.fifa.com/api/v3/calendar/matches'
        # base_url = 'https://api.fifa.com/api/v3/calendar/matches?from=2022-10-15T00:00:00Z&to=2022-10-16T23:59:59Z&language=en&count=500'
        response = requests.get(base_url,params=url_params,headers=headers)
        response_json = response.json()
        for result in response_json['Results']:
            
            match_date = result['Date'][:10].replace('-','_')
            output_date_folder = os.path.join(output_folder,match_date)
            
            match_date_folder = os.path.join(output_date_folder,'match')
            # Create directory if not exists: Suffix directory name
            mkdirpath = match_date_folder+'/'
            os.makedirs(mkdirpath, exist_ok=True)

            # Output file name
            output_file_name = os.path.join(match_date_folder,result['IdMatch']+'.json')
            print(output_file_name)
            # Write match info to file
            with open(output_file_name, "w") as outfile:
                
                json.dump(result, outfile)
            
            # Process timeline info for match
            IdCompetition = result['IdCompetition']
            IdSeason = result['IdSeason']
            IdStage = result['IdStage']
            IdMatch = result['IdMatch']
            # get_timeline_for_match(IdCompetition,IdSeason,IdStage,IdMatch,output_date_folder)
    
    except Exception as e: 
        print(e)
'''
Insert match list to sqlite db
'''
def db_insert_list_to_table(list_match_info,db_file_path):
    try:
        sqliteConnection = sqlite3.connect(db_file_path)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sqlite_insert_query = """INSERT INTO fifa_matches_log
                          (match_date, IdCompetition, IdSeason, IdStage, IdGroup,IdMatch,process_match,ts_process_match) 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?);"""

        cursor.executemany(sqlite_insert_query, list_match_info)
        sqliteConnection.commit()
        print("Total", cursor.rowcount, "Records inserted successfully into fifa_matches_log table")
        sqliteConnection.commit()
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert multiple records into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")

'''
Insert into matches table the list of matches for a particular day
Match information files are opened and data inserted into DB
'''
def db_insert_match_for_day(process_date,output_folder,db_file_path):
    str_process_date = process_date.strftime(r"%Y_%m_%d")
    dir_match_files = os.path.join(output_folder,str_process_date,'match')
    
    list_match_info = list()
    for match_file in glob(dir_match_files+'/*.json'):
        with open(match_file) as f:
            match_data = json.load(f)

            match_info = (  match_data['Date'][0:-1],
                            match_data['IdCompetition'],
                            match_data['IdSeason'],
                            match_data['IdStage'],
                            match_data['IdGroup'],
                            match_data['IdMatch'],
                            'Y',
                            datetime.datetime.now(timezone.utc).strftime(r"%Y-%m-%dT%H:%M:%S")
            )
        list_match_info.append(match_info)
    print(process_date)
    db_insert_list_to_table(list_match_info=list_match_info,db_file_path=db_file_path)


'''
Setup the SQLite database: 
Create database file if not currently exists
Create table if does not currently exists
'''
def db_setup():
    db_directory = 'db'
    db_filename = 'fifaProcessDB.db'
    # Create directory if not exists
    os.makedirs(os.path.dirname(db_directory+'/'), exist_ok=True)

    sql_create_table = "CREATE TABLE IF NOT EXISTS fifa_matches_log(\
                        match_date DATETIME NOT NULL,\
                        IdCompetition TEXT,\
                        IdSeason TEXT,\
                        IdStage TEXT,\
                        IdGroup TEXT,\
                        IdMatch TEXT NOT NULL,\
                        process_match CHARACTER(1) DEFAULT 'N' NOT NULL,\
                        process_timeline CHARACTER(1) DEFAULT 'N' NOT NULL,\
                        process_match_end_info CHARACTER(1) DEFAULT 'N' NOT NULL,\
                        ts_process_match DATETIME,\
                        ts_process_timeline DATETIME,\
                        ts_process_match_end_info DATETIME,\
                        PRIMARY KEY (match_date, IdCompetition,IdSeason,IdStage,IdGroup,IdMatch)\
                        );"
    db_file_path = os.path.join(db_directory,db_filename)
    with sqlite3.connect(db_file_path) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_create_table)
    return(db_file_path)

if __name__ == "__main__":
    db_file_path = db_setup()

    current_dirname = os.path.dirname(__file__)
    # find one directory above current dir
    base_dir = Path(current_dirname).parents[0]

    output_folder = os.path.join(base_dir,'data')


    base_date_diff = 0
    number_of_days_to_process = 5
    base = datetime.datetime.today()- datetime.timedelta(days=base_date_diff)
    date_list = [base - datetime.timedelta(days=x) for x in range(number_of_days_to_process)]

    for current_date in date_list:
        print(current_date)
        previous_date = current_date - datetime.timedelta(days=1)

        str_current_date = current_date.strftime('%Y-%m-%dT00:00:00Z')
        str_previous_date = previous_date.strftime('%Y-%m-%dT00:00:00Z')
        pass
        
        # from_date = '2022-10-16T00:00:00Z'
        # to_date = '2022-10-17T00:00:00Z'

        from_date = str_previous_date
        to_date = str_current_date

        count_records = 500
        language = 'en'

        input_params = {'from':from_date,
        'to':to_date,
        'count':count_records,
        'language':language
        }
        get_calendar_matches(url_params=input_params,output_folder=output_folder)
        db_insert_match_for_day(process_date=current_date,output_folder=output_folder,db_file_path=db_file_path)