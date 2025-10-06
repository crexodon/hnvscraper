from flask import Flask, render_template
import scraper

import folium
from folium.plugins import Realtime

app = Flask(__name__)

# @app.__init__
# def init_app():
#     print('Setting up Scraper...')
#     scraper.scrape_init()

@app.route("/")
def site():
    """Embed a map as an iframe on a page."""
    m = folium.Map(location=(49.142509, 9.208628), tiles="Cartodb Positron", zoom_start=13)

    vehicles = scraper.access_vehicles()

    #print(vehicles)

    for vehicle in vehicles:
        float_location = [float(vehicle['geojson']['coordinates'][1]), float(vehicle['geojson']['coordinates'][0])]
        popup_string = vehicle['line_number'] + ' > ' + vehicle['line_name']
        if (vehicle['vehicle_type'] == 1):
            folium.Marker(location=float_location, popup=popup_string).add_to(m) # Train
        elif (vehicle['vehicle_type'] == 5):
            folium.Marker(location=float_location, popup=popup_string).add_to(m) # Bus
        
    
    # set the iframe width and height
    m.get_root().width = "800px"
    m.get_root().height = "600px"
    iframe = m.get_root()._repr_html_()

    

    return render_template('template.html',iframe=iframe, vehicles=vehicles)

# @app.route('/api', methods=['GET'])
# def api():


if __name__ == "__main__":
    scraper.scrape_init()
    app.run(debug=True)