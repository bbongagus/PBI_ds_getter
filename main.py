import requests
from requests_ntlm import HttpNtlmAuth
from functools import partial
import json
import pandas as pd
import itertools
from urllib3.exceptions import InsecureRequestWarning

#format 'domain\\login' 
login = ''
password = ''

session = requests.Session()
url_ids = ''
url_ds = url_ids + "({})/DataSources"
session.auth = HttpNtlmAuth(login, password)
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

make_obj = lambda t: json.loads(t)
obj_value = lambda t: make_obj(t)['value']
get_apps_ids_from_url = lambda url: session.get(url, verify = False).text
get_app_ds_from_url = lambda url, param: session.get(url.format(param), verify=False).text
filter_dict_by = lambda d_col_ind,reg, d: dict(filter(lambda dict_el: re.search(reg, dict_el[d_col_ind]), d.items()))
filter_empty_vals = lambda d: list(filter(lambda v: 1 if len(v[1])>0 else 0, d))


get_ids = partial(get_apps_ids_from_url, url_ids)
get_app_ds = partial(get_app_ds_from_url, url_ds)
filter_dict_by_key = partial(filter_dict_by, 0)

get_ids_obj = lambda: obj_value(get_ids())
get_app_ds_obj = lambda i: obj_value(get_app_ds(i))


def flatten_dict(d):
    def items():
        for key, value in d.items():
            if isinstance(value, dict):
                for subkey, subvalue in flatten_dict(value).items():
                    yield key + "." + subkey, subvalue
            else:
                yield key, value
    return dict(items())
	
#getting dashboards Ids and clear id
ids_obj = get_ids_obj()
ids = [app['Id'] for app in ids_obj]

#getting data connections for every dashboard and placing it in tuple (AppId, [DataConnections])
ds_obj_arr = [(_id,get_app_ds_obj(_id)) for _id in ids]


#regular expr for needed attributes
reg = r"^Id$|^ConnectionString$|^DataModelDataSource\.(Type|Kind|Username)|^AppId$"

#clear empty values for empty apps
ds_obj_arr_filtered = filter_empty_vals(ds_obj_arr)

#Unpacking dataconnections from arrays and adding AppId to everyone of
sources = [{**{'AppId':ds_obj[0]}, **ds} for ds_obj in ds_obj_arr_filtered for ds in ds_obj[1]]

#flatten dataconnection attributes from multidict to flat dict
sources_flat = [flatten_dict(obj) for obj in sources]

#filter dataconnection object to get necessary attributes
sources_filtered = [filter_dict_by_key(reg, ds_obj_val) for ds_obj_val in sources_flat]

#making datafrome
dd = pd.DataFrame.from_dict(sources_filtered)

#saving data
path_csv = r''
dd.to_csv(path_csv)






	