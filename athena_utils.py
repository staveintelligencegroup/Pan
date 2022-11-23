from datetime import datetime, timedelta
from pyArango.connection import *
import pandas as pd
import configparser
config_ini = configparser.RawConfigParser()
config_ini.read("athena_config.ini")
database_config = {
    'arangoURL':f'{config_ini["server"]["server_ip"]}:{config_ini["server"]["arango_port"]}',
    'username': config_ini["server"]["username"],
    'password': config_ini["server"]["password"],
    'database' : f'{config_ini["server"]["database"]}',
}
conn = Connection(arangoURL=database_config['arangoURL'], username=database_config['username'],
                  password=database_config['password'])
db = conn['tweets']
event_types = ['Battles', 'Explosions/Remote violence', 'Protests', 'Riots', 'Violence against civilians', 'All']
factal_major_evebt_types = ['crime & courts','health','war & conflict']
factal_minor_event_types = ['protests & civil unrest','explosions','terrorism','stabbings','shootings','robberies','manhunts','kidnappings','bomb threats','barricade situations','AMBER Alerts']

daily_locales = [  'milan', 'nairobi', 'jakarta', 'bristol', 'minneapolis', 'cairo', 'caracas', 'santiago',
                   'berlin', 'nice', 'dc','beirut',  'grozny', 'lagos', 'richmond', 'algiers',
                   'newdelhi', 'london', 'kiev', 'belfast',  'sucre','gaza', 'bogota',
                   'karachi', 'durban', 'havana', 'portland', 'portauprince', 'melbourne', 'chicago',
                   'khartoum',  'managua', 'juba', 'donetsk', 'podgorica', 'kabul', 'guadalajara',
                   'saopaulo', 'colombo','istanbul','juarez', 'monterrey','minsk','doha','kinshasa','tripoli',]

def add_dashes_to_dates(date_list):
    date_list_fixed = []
    if type(date_list) is type([]):
        for date in date_list:
            date = str(date)
            date_list_fixed.append(date[0:4] + '-' + date[4:6] + '-' + date[6:8])
    else:
        date_list = [date_list]
        for date in date_list:
            date = str(date)
            date_list_fixed.append(date[0:4] + '-' + date[4:6] + '-' + date[6:8])
    if len(date_list) == 1:
        return date_list_fixed
    else:
        return date_list_fixed

def scaled_row_adder(row):
    val=0
    l = row.to_list()
    for i in l:
        val += (1-val)*i
    if val >1:
        return 1
    else:
        return val

def load_predDBData(zone, dates):
    score_db = db[f'{zone}_predictions']
    data = []
    evs = event_types+factal_minor_event_types+factal_major_evebt_types
    for date in dates:
        try:
            doc = score_db[date].getStore()
            doc['date']=pd.to_datetime(add_dashes_to_dates([date])[0])
            doc['created_at']=date
            data.append(doc)
        except:
            pass
    #This is a fillna but before it is in the DF because it needs it like this
    cols = [col for col in pd.DataFrame(data).columns if col in evs]
    for entry in data:
        for ev in evs:
            if ev not in entry:
                entry.update({ev:{'bound':0,'dates':[datetime.now().strftime('%Y-%m-%d')],'event_distribution':[0],'signal_value':0}})
    data = pd.DataFrame(data)
    cats = data.columns.to_list()
    categories = [cat for cat in cats if cat in evs]
    return data, categories


def get_preds(locale):
    locale = 'jakarta'

    daterange = pd.date_range(start=(datetime.now() - timedelta(days=200)),
                              end=(datetime.now() - timedelta(days=1)), freq='D')
    dates = [int(date.strftime('%Y%m%d')) for date in daterange]
    data, categories = load_predDBData(locale, dates,)
    cat_data = []
    for cat in categories:
        try:
            dta = data[cat].drop(data[data[cat] ==0].index)
            cat_data.append(pd.DataFrame.from_records(dta))
        except Exception as e:
            print(e)
    pred_dfs = []
    for cdata, cat in zip(cat_data, categories):
        preds = []
        datae = cdata[cdata['signal_value']!=0]

        if not datae.empty:
            ccols = datae.columns
            if 'date' in ccols:
                ddate = 'date'
            elif 'dates' in ccols:
                ddate = 'dates'
            datae = datae.dropna(subset=[ddate])

            for idx in datae.index:
                try:
                    d1 = cdata.loc[idx][ddate][0]
                    dr = [d.strftime('%Y-%m-%d') for d in pd.date_range(pd.to_datetime(d1), pd.to_datetime(d1)+timedelta(len(cdata.loc[idx]['event_distribution']))).to_list()]
                    preds.append(pd.DataFrame(list(zip(dr, cdata.loc[idx]['event_distribution']))
                                              , columns=[ddate, 'event']).set_index(ddate))
                except Exception as e:
                    print(f'{e}, handled I guess?')
            all_curves = pd.concat(preds, axis=1).fillna(0)
            all_curves['event_distribution'] = all_curves.apply(lambda row: scaled_row_adder(row), axis=1)
            pred_df = pd.DataFrame(all_curves['event_distribution'])
            pred_df['category'] = cat
            # pred_df = pred_df.drop(labels=['index', 'signal_value', 'bound'], axis=1)
            pred_dfs.append(pred_df)
    return pred_dfs