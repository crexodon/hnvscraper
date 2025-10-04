import sqlite3, requests, logging, re, urllib.parse, time, sys

vehicle_url = "https://www3.vvs.de/mnghnv/VELOC?"
vehicle_params = dict(
    CoordSystem = 'WGS84',
    LineID = '',
    coordOutputFormat = 'EPSG:4326',
    outputFormat = 'rapidJSON',
    serverInfo = '1'
)

station_version = '6'
station_lines = ["hnv:30001:E:R:j25", "hnv:30001:E:H:j25", "hnv:100S4: :H:j25", "hnv:100S4: :R:j25"]
station_url = "https://www3.vvs.de/mnghnv/XML_GEOOBJECT_REQUEST?"
station_params = dict(
    SpEncId = '0',
    line = '',
    outputFormat = 'rapidJSON',
    serverInfo = '1',
    coordOutputFormat = 'EPSG:4326',
    spTZO = '1',
    stFaZon = '1',
    vSL = '1',
    version = '10.2.10.139'
)

timetable_url = "https://www3.vvs.de/mnghnv/XML_DM_REQUEST?"
timetable_params = dict(
    SpEncId = '0',
    mode = 'direct',
    name_dm = '',
    outputFormat = 'rapidJSON',
    type_dm = 'stop',
    useRealtime = '0',
    coordOutputFormat = 'EPSG:4326',
    limit = '40',
    version = '10.2.10.139',
    itdDateDay = '5',
    itdTime = '04:00',
    useProxFootSearch = '0'
)

stop_reg = re.compile(r'^.*?(?=#)')

conn = sqlite3.connect('testdb.db')

def create_database():
    conn = sqlite3.connect('testdb.db')
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS busses (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 line_id TEXT,
                 bus_id TEXT,
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

# Line ID has a :6 at the end!
def get_stations(station_line):
    station_params['line'] = station_line + ':' + station_version
    station_params_str = urllib.parse.urlencode(station_params, safe=':')
    resp = requests.get(url=station_url, params=station_params_str)
    data: dict = resp.json()

    sequences = data['transportations'][0]['locationSequence']

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
            line_id = data['transportations'][0]['id'] 
        ) 

        c = conn.cursor()
        columns = ', '.join(station_parse.keys())
        placeholders = ', '.join('?' * len(station_parse))
        sql = 'INSERT OR IGNORE INTO stations ({}) VALUES ({})'.format(columns, placeholders)
        c.execute(sql, tuple(station_parse.values()))

        conn.commit()

# TODO Needs further adjustment, Weekday, Saturday, Sun and Holiday timetables            
def get_timetables(station_id):
    timetable_params['name_dm'] = station_id
    timetable_params_str = urllib.parse.urlencode(timetable_params, safe=':')
    resp = requests.get(url=timetable_url, params=timetable_params_str)
    data: dict = resp.json()

    times = data['stopEvents']
    for time in times:

        hints = []
        if('hints' in time):
            hints = time['hints']

        time_parse = dict(
            station_id = time['location']['id'],
            stop_id = time['location']['properties']['stopId'],
            timetable_timestamp = time['departureTimeBaseTimetable'],
            departure = time['departureTimePlanned'],
            line_id = time['transportation']['id'],
            hints = hints
        )

        print(dict(time_parse))

# Transforms Line ID from Scrape to Station format and also the other way
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

def scrape(vehicle_line):
    vehicle_params['LineID'] = transform_lineid(vehicle_line)
    vehicle_params_str = urllib.parse.urlencode(vehicle_params, safe=':')
    resp = requests.get(url=vehicle_url, params=vehicle_params_str)
    data = resp.json()

    print('Current Vehicles on: ' + vehicle_line + ' = ' + str(len(data)))

    for vec in data:
        print(vec['DirectionText'] + ", ID: " + vec['ID'])
        

        vec_parse = dict(
            line_id = vehicle_line,
            bus_id = vec['ID'],
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

        c = conn.cursor()

        columns = ', '.join(vec_parse.keys())
        placeholders = ', '.join('?' * len(vec_parse))
        sql = 'INSERT INTO busses ({}) VALUES ({})'.format(columns, placeholders)
        c.execute(sql, tuple(vec_parse.values()))

        conn.commit()

if __name__ == '__main__':
    # import http.client as http_client
    # http_client.HTTPConnection.debuglevel = 1

    # #You must initialize logging, otherwise you'll not see debug output.
    # logging.basicConfig()
    # logging.getLogger().setLevel(logging.DEBUG)
    # requests_log = logging.getLogger("requests.packages.urllib3")
    # requests_log.setLevel(logging.DEBUG)
    # requests_log.propagate = True
    
    create_database()
    for station_line in station_lines:
        print('Getting Station Info for: ' + station_line)
        get_stations(station_line)
        time.sleep(5)
    
    #get_timetables('de:08121:191:0:1')

    try:
        while True:
            for station_line in station_lines:
                scrape(station_line)
                time.sleep(2)
    except KeyboardInterrupt:
        print('KeyboardInterrupt detected. Cleaning up and exiting...')
        conn.close()
        sys.exit()
    