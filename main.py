from flask import Flask, render_template_string
import scraper

import folium
from folium.plugins import Realtime

app = Flask(__name__)

@app.route("/")
def site():
    """Embed a map as an iframe on a page."""
    m = folium.Map(location=(49.126131, 9.270954), tiles="Cartodb Positron", zoom_start=12)

    # set the iframe width and height
    m.get_root().width = "800px"
    m.get_root().height = "600px"
    iframe = m.get_root()._repr_html_()

    return render_template_string(
        """
            <!DOCTYPE html>
            <html>
                <head></head>
                <body>
                    <h1>HNV Scraper</h1>
                    {{ iframe|safe }}
                </body>
            </html>
        """,
        iframe=iframe,
    )

# @app.route('/api', methods=['GET'])
# def api():


if __name__ == "__main__":
    scraper.scrape_init()
    app.run(debug=True)