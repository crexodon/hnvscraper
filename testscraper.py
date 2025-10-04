import sqlite3, requests, logging, re, urllib.parse

bus_lines = ["j25-hnv-30001-R-E", "j25-hnv-30001-H-E"]
s4_lines = ["j25-hnv-100S4-R-"]
bus_url = "https://www3.vvs.de/mnghnv/VELOC?"
bus_params = dict(
    CoordSystem = 'WGS84',
    LineID = '',
    #coordOutputFormat = 'EPSG:4326',
    outputFormat = 'rapidJSON',
    serverInfo = '1'
)

station_lines = ["hnv:30001:E:R:j25:6", "hnv:30001:E:H:j25:6"]
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
def get_stations(bus_line):
    station_params['line'] = bus_line
    station_params_str = urllib.parse.urlencode(station_params, safe=':')
    resp = requests.get(url=station_url, params=station_params_str)
    data: dict = resp.json()

    sequences = data['transportations'][0]['locationSequence']

    conn = sqlite3.connect('testdb.db')

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
    conn.close()

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

def scrape(bus_line):
    bus_params['LineID'] = bus_line
    resp = requests.get(url=bus_url, params=bus_params)
    data = resp.json()

    conn = sqlite3.connect('testdb.db')

    # Convert line_id format from j25-hnv-30001-R-E to hnv:30001:R:E:j25
    line_reg = re.sub(r'^(\w+)-(.*)', r'\2-\1', bus_line) # AI generated regex
    line_reg = line_reg.replace('--', '-')
    bus_line = line_reg.replace('-', ':')

    print('Current Busses on: ' + bus_line + ' = ' + str(len(data)))

    for bus in data:
        print(bus['DirectionText'] + ", ID: " + bus['ID'])
        

        bus_parse = dict(
            line_id = bus_line,
            bus_id = bus['ID'],
            bus_timestamp = bus['Timestamp'],
            latitude = bus['Latitude'],
            longitude = bus['Longitude'],
            current_stop = stop_reg.match(bus['CurrentStop']).group(),
            next_stop = stop_reg.match(bus['NextStop']).group(),
            realtime_available = bus['RealtimeAvailable'],
            current_delay = bus['Delay'],
            bus_name = bus['LineText'],
            bus_journey = bus['DirectionText']
        )

        c = conn.cursor()

        columns = ', '.join(bus_parse.keys())
        placeholders = ', '.join('?' * len(bus_parse))
        sql = 'INSERT INTO busses ({}) VALUES ({})'.format(columns, placeholders)
        c.execute(sql, tuple(bus_parse.values()))

        conn.commit()

    conn.close()




        



if __name__ == '__main__':
    import http.client as http_client
    http_client.HTTPConnection.debuglevel = 1

    # You must initialize logging, otherwise you'll not see debug output.
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True
    
    create_database()
    get_stations(station_lines[0])
    #get_timetables('de:08121:191:0:1')
    scrape(bus_line=bus_lines[0])


#FOREIGN KEY(trackartist) REFERENCES artist(artistid)