import geopandas as gpd
import numpy as np
import plotly.graph_objects as go
import json
from athena_utils import *
from geopy.geocoders import Nominatim
from tqdm import tqdm

def get_coords(locale):
    geolocator = Nominatim(user_agent='stave')
    if locale == 'saopaulo':
        loc = 'sao paulo'
    elif locale == 'newdelhi':
        loc = 'new delhi'
    elif locale == 'addisababa':
        loc = 'addis ababa'
    elif locale == 'portauprince':
        loc = 'port au prince'
    elif locale == 'ottowa':
        loc = 'ottawa'
    elif locale == 'santiago':
        loc = 'santiago, chile'
    else:
        loc = locale

    location = geolocator.geocode(loc)
    return location.latitude, location.longitude

geodf = gpd.read_file('maps/ne_10m_urban_areas.shp')
provinces = gpd.read_file('maps/ne_10m_admin_1_states_provinces.shp')
counties = gpd.read_file('maps/ne_10m_admin_2_counties.shp')
counties["threat"] = list(np.random.randint(0, 10, len(counties)-1))+[100]
geodf["threat"] = list(np.random.randint(20, 40, len(geodf)-1))+[100]
provinces["threat"] =  list(np.random.randint(0, 10, len(provinces)-1))+[100]

# shape file is a different CRS,  change to lon/lat GPS co-ordinates
#geodf = geodf.to_crs("WGS84").set_index("Posno")
fig = go.Figure()
fig.add_trace(go.Choroplethmapbox(geojson=json.loads(provinces.to_json()),
                                    locations=provinces.index, z=provinces['threat'],
                                    colorscale="Inferno", marker_line_width=.5))
fig.add_trace(go.Choroplethmapbox(geojson=json.loads(geodf.to_json()),
                                    locations=geodf.index, z=geodf['threat'],
                                    colorscale="Inferno", marker_line_width=.5))
fig.add_trace(go.Choroplethmapbox(geojson=json.loads(counties.to_json()),
                                    locations=counties.index, z=counties['threat'],
                                    colorscale="Inferno", marker_line_width=.5))

for locale in tqdm(daily_locales):
    #preds = get_preds(locale)
    #all = preds[0]
    loc = get_coords(locale)
    #pp = provinces[provinces.latitude < loc[1]+0.1 and provinces.latitude > loc[1]-0.1 ]# provinces.longitude < loc[3]+0.1 and provinces.longitude > loc[3]-0.1]
    x = provinces[provinces.latitude < loc[0]+1.1]
    xx = x[x.latitude > loc[0]-1.1]
    xxx = xx[xx.longitude < loc[1]+1.1]
    xxxx = xxx[xxx.longitude > loc[1]-1.1]
    xxxx['threat'] = np.random.randint(80, 100,len(xxxx))
    fig.add_trace(go.Choroplethmapbox(geojson=json.loads(xxxx.to_json()),
                                        locations=xxxx.index, z=xxxx['threat'],
                                        colorscale="Inferno", marker_line_width=.5))
    #x = geodf[geodf.latitude < loc[0]+1.1]
    #xx = x[x.latitude > loc[0]-1.1]
    #xxx = xx[xx.longitude < loc[1]+1.1]
    #xxxx = xxx[xxx.longitude < loc[1]+1.1]
    #xxxx['threat'] = [100]*len(xxxx)
    #fig.add_trace(go.Choroplethmapbox(geojson=json.loads(xxxx.to_json()),
    #                                    locations=xxxx.index, z=xxxx['threat'],
    #                                    colorscale="Inferno", marker_line_width=.5))

fig.update_layout(mapbox_style="carto-darkmatter",
                        height = 1000,
                        autosize=True,
                        margin={"r":0,"t":0,"l":0,"b":0},
                        paper_bgcolor='#303030',
                        plot_bgcolor='#303030',
                        mapbox=dict(center=dict(lat=60.1699, lon=24.9384),zoom=9),
                        )

fig.show()