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

# Stores last access vehicles from get_vehicles. Used by frontend
last_vecs = []

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

# TODO Needs further adjustment, Weekday, Saturday, Sun and Holiday timetables            
# def get_timetables(station_id):
#     timetable_params['name_dm'] = station_id
#     timetable_params_str = urllib.parse.urlencode(timetable_params, safe=':')
#     resp = requests.get(url=timetable_url, params=timetable_params_str)
#     data: dict = resp.json()

#     times = data['stopEvents']
#     for time in times:

#         hints = []
#         if('hints' in time):
#             hints = time['hints']

#         time_parse = dict(
#             station_id = time['location']['id'],
#             stop_id = time['location']['properties']['stopId'],
#             timetable_timestamp = time['departureTimeBaseTimetable'],
#             departure = time['departureTimePlanned'],
#             line_id = time['transportation']['id'],
#             hints = hints
#         )

#         print(dict(time_parse))

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
        resp = requests.get(url=vehicle_config['station_url'], params=vehicle_params_str)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print('ERROR while GET: ' + str(err))
    except requests.exceptions.RequestException as err:
        print('ERROR while GET: ' + str(err))

    data = resp.json()

    conn = sqlite3.connect('scraped.db')

    last_vecs.clear()

    # TODO Add more robust keyhandling incase of bad data
    for vec in data:

        vec_parse = dict(
            line_id = line,
            trip_id = vec['JourneyIdentifier'],
            bus_timestamp = vec['Timestamp'],
            latitude = vec['Latitude'],
            longitude = vec['Longitude'],
            current_stop = stop_reg.match(vec['CurrentStop']).group(),
            next_stop = stop_reg.match(vec['NextStop']).group(),
            realtime_available = vec['RealtimeAvailable'],
            current_delay = vec['Delay'],
            bus_name = vec['LineText'],
            bus_journey = vec['DirectionText']
        )

        last_vecs.append(vec_parse)

        c = conn.cursor()

        # TODO Add more robust sql access handling
        columns = ', '.join(vec_parse.keys())
        placeholders = ', '.join('?' * len(vec_parse))
        sql = 'INSERT INTO vehicles ({}) VALUES ({})'.format(columns, placeholders)
        c.execute(sql, tuple(vec_parse.values()))

        conn.commit()
    
    conn.close()

# I could use the the last_vecs dict directly, however this is for sql demonstration
# Later on this can be used to access historical data from the database!
# Returns list of data with geojson point, timestamp, current and next stop (parsed) and line info
def access_data():
    conn = sqlite3.connect('scraped.db')

    c = conn.cursor()

    select_query = """SELECT latitude, longitude, bus_timestamp, current_stop, next_stop, realtime_available, current_delay, bus_name, bus_journey FROM vehicles WHERE trip_id = (?) AND bus_timestamp = (?);"""
    rets = []
    for last_vec in last_vecs:
        c.execute(select_query, last_vec)
        ret = c.fetchall()

        c.execute("""SELECT name FROM stations WHERE stop_id = (?)""", (ret[0][3], ))
        cur_stop = c.fetchall()

        c.execute("""SELECT name FROM stations WHERE stop_id = (?)""", (ret[0][4], ))
        nxt_stop = c.fetchall()
        
        ret_parse = dict(
            geojson = dict(
                type = "Point",
                coordinates = [
                    ret[0][1],
                    ret[0][0]
                ]
            ),
            timestamp = ret[0][2],
            current_stop = cur_stop[0][0],
            next_stop = nxt_stop[0][0],
            realtime_available = str(ret[0][5]),
            realtime_delay = str(ret[0][6]),
            line_number = ret[0][7],
            line_name = ret[0][8]
        )

        rets.append(ret_parse)
    
    conn.close()

    return rets

def scrape():
    while True:
        for line in lines:
            scrape(line)
            time.sleep(3)
    
    
def scrape_init():
    create_database()
    for line in lines:
        print('Getting Station Info for: ' + line)
        get_stations(line)
        time.sleep(5)

    t = threading.Thread(target=scrape)
    t.start()
