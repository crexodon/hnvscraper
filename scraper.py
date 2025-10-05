import sqlite3, requests 
import re, urllib.parse
import time, sys, json

lines = json.load('lines.json')
config = json.load('config.json')

# Compiled RegEx for StationID Lookup
stop_reg = re.compile(r'^.*?(?=#)')

def create_database():
    conn = sqlite3.connect('scraped.db')
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS vehicles (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 line_id TEXT,
                 trip_id TEXT,
                 get_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 bus_timestamp TIMESTAMP,
                 latitude TEXT,
                 longitude TEXT,
                 current_stop TEXT,
                 next_stop TEXT,
                 realtime_available BOOLEAN DEFAULT FALSE,
                 current_delay INTEGER,
                 bus_name TEXT,
                 bus_journey TEXT)''')
    conn.commit()

    #stop_id 5400034
    c.execute('''CREATE TABLE IF NOT EXISTS stations (
                 station_id TEXT PRIMARY KEY,
                 get_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 name TEXT,
                 place_id TEXT,
                 place_name TEXT,
                 latitude TEXT,
                 longitude TEXT,
                 stop_id TEXT,
                 validity_from TEXT,
                 validity_to TEXT,
                 line_id TEXT)''')
    conn.commit() 

    # See comment at get_timetable
    #line_id hnv:30001:E:H:j25
    #stop_id 5400034
    # c.execute('''CREATE TABLE IF NOT EXISTS timetables (
    #              id INTEGER PRIMARY KEY AUTOINCREMENT,
    #              station_id TEXT,
    #              stop_id TEXT,
    #              get_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #              timetable_timestamp TIMESTAMP,
    #              departure TIMESTAMP,
    #              line_id TEXT,
    #              hints TEXT)''')
    # conn.commit()

    conn.close()

# Only call this once, populates db with all possible stations from given line
def get_stations(line):
    station_config = config['station_config']
    station_config['station_params']['lineID'] = line + lines.get(line,)['line_version']
    
    station_config_str = urllib.parse.urlencode(station_config, safe=':')
    try:
        resp = requests.get(url=station_config['station_url'], params=station_config_str)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print('ERROR while GET: ' + str(err))
    except requests.exceptions.RequestException as err:
        print('ERROR while GET: ' + str(err))
    
    data = resp.json()

    # TODO Add more robust keyhandling incase of bad data
    sequences = data['transportations'][0]['locationSequence']

    conn = sqlite3.connect('scraped.db')

    for station in sequences:
        station_parse = dict(
            station_id = station['id'],
            name = station['name'],
            place_id = station['parent']['id'],
            place_name = station['parent']['name'],
            latitude = station['coord'][0],
            longitude = station['coord'][1],
            stop_id = station['parent']['properties']['stopId'],
            validity_from = data['transportations'][0]['properties']['validity']['from'],
            validity_to = data['transportations'][0]['properties']['validity']['to'],
            #line_id = data['transportations'][0]['id']
            line_id = line
        ) 

        # TODO Add more robust sql access handling
        c = conn.cursor()
        columns = ', '.join(station_parse.keys())
        placeholders = ', '.join('?' * len(station_parse))
        sql = 'INSERT OR IGNORE INTO stations ({}) VALUES ({})'.format(columns, placeholders)
        c.execute(sql, tuple(station_parse.values()))

        conn.commit()
    
    conn.close()

