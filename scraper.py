import sqlite3, requests 
import re, urllib.parse
import time, sys, json
import threading

with open('lines.json') as f:
    lines = json.load(f)
with open('config.json') as f:
    config = json.load(f)

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
                 vehicle_timestamp TIMESTAMP,
                 latitude TEXT,
                 longitude TEXT,
                 current_stop TEXT,
                 next_stop TEXT,
                 realtime_available BOOLEAN DEFAULT FALSE,
                 current_delay INTEGER,
                 vehicle_name TEXT,
                 vehicle_journey TEXT,
                 vehicle_type TEXT)''')
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
    station_config['station_params']['line'] = line + ':' + lines.get(line,)['line_version']
    
    station_params_str = urllib.parse.urlencode(station_config['station_params'], safe=':')
    try:
        resp = requests.get(url=station_config['station_url'], params=station_params_str)
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

# Transforms Line ID from Scrape to Station format and also the other way
# TODO Add catch when transforming lineid from get_station request, the raw output adds the line_version to the end of the line_id!
def transform_lineid(line_str):

    if ':' in line_str:
        match = re.match(r'^([^:]+):([^:]+):([^:]*):([^:]+):([^:]+)$', line_str)

        if match:
            line1 = match.group(1)    # hnv
            line2 = match.group(2)    # 30001
            dir2 = match.group(3)     # E or space
            dir1 = match.group(4)     # R or H
            prefix = match.group(5)   # j25
            
            # If pos2 is space, replace with empty string
            if dir2 == ' ':
                dir2 = ''
            
            # Reconstruct: prefix-part1-part2-pos1-pos2
            return f"{prefix}-{line1}-{line2}-{dir1}-{dir2}"
    
    elif '-' in line_str:
        match = re.match(r'^([^-]+)-(.+)-([^-]+)-([^-]*)$', line_str)
        
        if match:
            prefix = match.group(1)  # j25
            line = match.group(2)    # hnv-30001
            dir1 = match.group(3)    # R or H
            dir2 = match.group(4)    # E or empty string
            
            # If pos2 is empty, replace with space
            if not dir2:
                dir2 = ' '
            
            # Reconstruct: middle:number:pos2:pos1:prefix
            # First replace the dash in middle with colon
            line = line.replace('-', ':')
            
            # Build final string with swapped positions
            return f"{line}:{dir2}:{dir1}:{prefix}"
    
    return None

# Gets current vehicles on given line. Make sure to ratelimit!
def get_vehicles(line):
    vehicle_config = config['vehicle_config']
    vehicle_config['vehicle_params']['LineID'] = transform_lineid(line)
    
    vehicle_params_str = urllib.parse.urlencode(vehicle_config['vehicle_params'], safe=':')
    try:
        resp = requests.get(url=vehicle_config['vehicle_url'], params=vehicle_params_str)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print('ERROR while GET: ' + str(err))
    except requests.exceptions.RequestException as err:
        print('ERROR while GET: ' + str(err))
    
    if not resp:
        return

    data = resp.json()

    conn = sqlite3.connect('scraped.db')

    # TODO Add more robust keyhandling incase of bad data
    for vec in data:

        vec_parse = dict(
            line_id = line,
            trip_id = vec['JourneyIdentifier'],
            vehicle_timestamp = vec['Timestamp'],
            latitude = vec['Latitude'],
            longitude = vec['Longitude'],
            current_stop = stop_reg.match(vec['CurrentStop']).group(),
            next_stop = stop_reg.match(vec['NextStop']).group(),
            realtime_available = vec['RealtimeAvailable'],
            current_delay = vec['Delay'],
            vehicle_name = vec['LineText'],
            vehicle_journey = vec['DirectionText'],
            vehicle_type = vec['MOTCode']
        )

        c = conn.cursor()

        # TODO Add more robust sql access handling
        columns = ', '.join(vec_parse.keys())
        placeholders = ', '.join('?' * len(vec_parse))
        sql = 'INSERT INTO vehicles ({}) VALUES ({})'.format(columns, placeholders)
        c.execute(sql, tuple(vec_parse.values()))

        conn.commit()
    conn.close()

    return resp.status_code

# I could use the the last_vecs dict directly, however this is for sql demonstration
# Later on this can be used to access historical data from the database!
# Returns list of data with geojson point, timestamp, current and next stop (parsed) and line info
def access_vehicles():
    conn = sqlite3.connect('scraped.db')

    c = conn.cursor()

    select_query = """SELECT latitude, longitude, vehicle_timestamp, current_stop, next_stop, realtime_available, current_delay, vehicle_name, vehicle_journey, vehicle_type, get_timestamp 
                      FROM vehicles 
                      WHERE get_timestamp = (SELECT MAX(get_timestamp) FROM vehicles WHERE line_id = (?));"""
    rets = []
    for line in lines:
        c.execute(select_query, (line, ))
        ret = c.fetchall()

        print('Fetched ' + str(len(ret)) + ' vehicles on line: ' + str(line))

        for vehicle in ret:
            c.execute("""SELECT name FROM stations WHERE stop_id = (?)""", (vehicle[3], ))
            cur_stop = c.fetchone()

            c.execute("""SELECT name FROM stations WHERE stop_id = (?)""", (vehicle[4], ))
            nxt_stop = c.fetchone()
            
            ret_parse = dict(
                geojson = dict(
                    type = "Point",
                    coordinates = [
                        vehicle[1],
                        vehicle[0]
                    ]
                ),
                vehicle_timestamp = vehicle[2],
                current_stop = cur_stop[0],
                next_stop = nxt_stop[0],
                realtime_available = 'yes' if vehicle[5] == 1 else 'no',
                realtime_delay = str(vehicle[6]) + 's',
                line_number = vehicle[7],
                line_name = vehicle[8],
                line_type = vehicle[9],
                get_timestamp = vehicle[10]
            )

            rets.append(ret_parse)
    
    conn.close()

    return rets

# TODO Add function to access stations for frontend
# def access_stations():
#     conn = sqlite3.connect('scraped.db')

#     c = conn.cursor()
    
#     select_query = """SELECT latitude, longitude, name FROM stations WHERE line_id = (?)"""
#     rets = []

#     for line in lines:
        
    
#     return rets

def scrape_run():
    print("Starting Scraper Thread...")
    while True:
        for line in lines:
            ret = get_vehicles(line)
            print('Scraping ' + str(line) + ' > ' + str(ret))
            time.sleep(3)
        
    
    
def scrape_init():
    print("Starting Scraper Init...")
    create_database()
    # for line in lines:
    #     print('Getting Station Info for: ' + line)
    #     get_stations(line)
    #     time.sleep(2)

    t = threading.Thread(target=scrape_run)
    t.start()
