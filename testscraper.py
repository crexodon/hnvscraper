import sqlite3, requests, logging, re

bus_lines = ["j25-hnv-30001-R-E", "j25-hnv-30001-H-E"]
s4_lines = ["j25-hnv-100S4-R-"]
bus_url = "https://www3.vvs.de/mnghnv/VELOC?"
bus_params = dict(
    CoordSystem = 'WGS84',
    LineID = '',
    #coordOutputFormat = 'EPSG:4326',
    outputFormat = 'rapidJSON',
    serverInfo = 1
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
                 longitude TEXT
                 stop_id)''')
    conn.commit() 

    #line_id hnv:30001:E:H:j25
    #stop_id 5400034
    c.execute('''CREATE TABLE IF NOT EXISTS timetables (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 station_id TEXT,
                 stop_id TEXT,
                 get_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 timetable_timestamp TIMESTAMP,
                 departure TIMESTAMP,
                 line_id TEXT)''')
    conn.commit()

    conn.close()

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
    # import http.client as http_client
    # http_client.HTTPConnection.debuglevel = 1

    # # You must initialize logging, otherwise you'll not see debug output.
    # logging.basicConfig()
    # logging.getLogger().setLevel(logging.DEBUG)
    # requests_log = logging.getLogger("requests.packages.urllib3")
    # requests_log.setLevel(logging.DEBUG)
    # requests_log.propagate = True
    
    create_database()
    scrape(bus_line=bus_lines[0])


#FOREIGN KEY(trackartist) REFERENCES artist(artistid)