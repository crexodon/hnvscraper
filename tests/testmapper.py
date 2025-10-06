import sqlite3, folium
from geopy.distance import geodesic
from datetime import datetime

m = folium.Map(location=(49.126131, 9.270954), tiles="Cartodb Positron")

conn = sqlite3.connect('tests/testdb2.db')

c = conn.cursor()

select_query = """SELECT latitude, longitude FROM stations WHERE line_id = (?);"""

c.execute(select_query, ('hnv:30001:E:R:j25', ))

items = c.fetchall()

vehicle_markers = folium.FeatureGroup("Vehicle Points").add_to(m)
station_markers = folium.FeatureGroup("Stations").add_to(m)

for item in items:
    float_item = [float(item[0]), float(item[1])]
    folium.Marker(location = float_item, icon=folium.Icon("red")).add_to(station_markers)

# ------

select_query = """SELECT latitude, longitude, bus_timestamp FROM vehicles WHERE trip_id = (?);"""
c.execute(select_query, ('8225#!ADD!#vdvserversws##!ADD!#DDIP-NVBW-VVS#', ))

items = c.fetchall()

# Testdata
last_vecs = [
    ("8223#!ADD!#vdvserversws##!ADD!#DDIP-NVBW-VVS#", "2025-10-05T16:57:23+02:00"),
    ("85348-48899#!ADD!#vbk##!ADD!#DDIP-M_KVV##!ADD!#DDIP-NVBW-VVS#", "2025-10-05T16:57:23+02:00")
]

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

    print(ret_parse)
conn.close()


# for item in items:
#     float_item = [float(item[0]), float(item[1])]
#     folium.Marker(location= float_item, icon=folium.Icon("blue")).add_to(vehicle_markers)

float_items = []
prev_item = []
mark_distance = []
timestamps = []
prev_timestamp = []
for item in items:
    float_item = [float(item[0]), float(item[1])]
    float_items.append(float_item)

    if not prev_item:
        prev_item = float_item
        continue

    
    distance = geodesic(prev_item, float_item).km
    print(distance)
    mark_distance.append(distance)

    current_stamp = datetime.fromisoformat(item[2])
    
    if not prev_timestamp:
        prev_timestamp = current_stamp
        continue

    # WTF, Bus Timestamps seem to glitch to different dates sometimes!
    # -> testdb id 47
    delta_d = current_stamp - prev_timestamp
    print(delta_d)
    
    prev_timestamp = current_stamp
    prev_item = float_item

print(len(float_items))
print(len(mark_distance))

#folium.ColorLine(positions=float_items, colors=mark_distance, colormap=["y", "orange", "r"], weight=5).add_to(m)
folium.PolyLine(locations=float_items).add_to(m)

folium.Marker(location=float_items[0], icon=folium.Icon(color="blue"), popup="Start Bus 1").add_to(vehicle_markers)
folium.Marker(location=float_items[len(float_items)-1], icon=folium.Icon(color="green"), popup="End Bus 1").add_to(vehicle_markers)


folium.LayerControl().add_to(m)

m.save("visual.html")
