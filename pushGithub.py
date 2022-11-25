from pymongo import MongoClient
import yaml
from functools import reduce
import pandas as pd
import numpy as np
import subprocess
from git import Repo

with open("/root/wdash/config.yml", "r") as config_file:
    CONFIG = yaml.safe_load(config_file)

HOST = CONFIG["DB_HOST"]
PORT = CONFIG["DB_PORT"]
NAME = CONFIG["DB_NAME"]
INTERVAL = CONFIG["INTERVAL"]
SERVER_PORT = CONFIG["PORT"]
WINDY_EMBED = CONFIG["WINDY_EMBED"]

mc = MongoClient(host=HOST, port=PORT)
cw = mc[NAME].weather_current
cf = mc[NAME].weather_forecasted
ca = mc[NAME].aqi_current
caf = mc[NAME].aqi_forecasted

def recursive_get(d, *keys):
    return reduce(lambda c, k: c.get(k, {}), keys, d)

def weathers2df(weathers):
    keys_flat = []
    for w in weathers:
        for k, v in w.items():
            if isinstance(v, dict):
                for kn, vn in v.items():
                    key_flat = ".".join((k, kn))
                    keys_flat.append(key_flat)
            else:
                keys_flat.append(k)
    keys_flat = sorted(list(set(keys_flat)))
    keys_args = [tuple(k.split(".")) for k in keys_flat]
    data = {k: [np.nan] * len(weathers) for k in keys_flat}
    for i, w in enumerate(weathers):
        for j, k in enumerate(keys_args):
            v = recursive_get(w, *k)
            key_flat = keys_flat[j]
            if v:
                data[key_flat][i] = v
    return pd.DataFrame(data)

f = [i for i in cf.find({})]
fdf = weathers2df(f)
fdf["origin"] = "forecast"
fdf['datetime'] = pd.DatetimeIndex(pd.to_datetime(fdf['datetime'])).tz_localize('UTC').tz_convert('Asia/Kuala_Lumpur')
fdf['datetime'] = pd.DatetimeIndex([i.replace(tzinfo=None) for i in fdf['datetime']])
fdf['fetched_at'] = pd.DatetimeIndex(pd.to_datetime(fdf['datetime'])).tz_localize('UTC').tz_convert('Asia/Kuala_Lumpur')
fdf['fetched_at'] = pd.DatetimeIndex([i.replace(tzinfo=None) for i in fdf['datetime']])
fdf.to_csv('/root/pushGithub/forecast.csv',index=False)

h = [i for i in cw.find({})]
hdf = weathers2df(h)
hdf["origin"] = "history"
hdf['datetime'] = pd.DatetimeIndex(pd.to_datetime(hdf['datetime'])).tz_localize('UTC').tz_convert('Asia/Kuala_Lumpur')
hdf['datetime'] = pd.DatetimeIndex([i.replace(tzinfo=None) for i in hdf['datetime']])
hdf['fetched_at'] = pd.DatetimeIndex(pd.to_datetime(hdf['datetime'])).tz_localize('UTC').tz_convert('Asia/Kuala_Lumpur')
hdf['fetched_at'] = pd.DatetimeIndex([i.replace(tzinfo=None) for i in hdf['datetime']])
hdf.to_csv('/root/pushGithub/history.csv',index=False)

f = [i for i in caf.find({})]
afdf = weathers2df(f)
afdf["origin"] = "forecast"
afdf['datetime'] = pd.DatetimeIndex(pd.to_datetime(afdf['datetime'])).tz_localize('UTC').tz_convert('Asia/Kuala_Lumpur')
afdf['datetime'] = pd.DatetimeIndex([i.replace(tzinfo=None) for i in afdf['datetime']])
afdf['fetched_at'] = pd.DatetimeIndex(pd.to_datetime(afdf['datetime'])).tz_localize('UTC').tz_convert('Asia/Kuala_Lumpur')
afdf['fetched_at'] = pd.DatetimeIndex([i.replace(tzinfo=None) for i in afdf['datetime']])
afdf.to_csv('/root/pushGithub/aqi_forecast.csv',index=False)

h = [i for i in ca.find({})]
ahdf = weathers2df(h)
ahdf["origin"] = "history"
ahdf['datetime'] = pd.DatetimeIndex(pd.to_datetime(ahdf['datetime'])).tz_localize('UTC').tz_convert('Asia/Kuala_Lumpur')
ahdf['datetime'] = pd.DatetimeIndex([i.replace(tzinfo=None) for i in ahdf['datetime']])
ahdf['fetched_at'] = pd.DatetimeIndex(pd.to_datetime(ahdf['datetime'])).tz_localize('UTC').tz_convert('Asia/Kuala_Lumpur')
ahdf['fetched_at'] = pd.DatetimeIndex([i.replace(tzinfo=None) for i in ahdf['datetime']])
ahdf.to_csv('/root/pushGithub/aqi_history.csv',index=False)

subprocess.call('cd /root/pushGithub/;git add .;git commit -m "new data updated";git push', shell=True)
# subprocess.call('git add .', shell=True)
# subprocess.call('git commit -m "new data updated"', shell=True)
# subprocess.call('git push', shell=True)