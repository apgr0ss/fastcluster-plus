import sys
import os
import json
from getpass import getpass
import psycopg2
import numpy as np
import pandas as pd

def spec_exists(query_dict, new_entry):
    for key, val in query_dict.items():
        if val == new_entry:
            return key
    return None

def gen_spec_name(query):
    if os.path.exists('output/query_dict.json'):
        with open('output/query_dict.json', 'r') as f:
            query_dict = json.load(f)
        with open('output/query_dict_temp.json', 'w') as f:
            json.dump(query_dict, f, indent=4)
    else:
        query_dict = {}

    query = query.replace('\t',' ')
    new_id = spec_exists(query_dict, query)
    if new_id is None:
        ids = list(query_dict.keys())
        new_id = np.random.randint(10000)
        while new_id in ids:
            new_id = np.random.randint(10000)
        query_dict[new_id] = query
        with open('output/query_dict.json', 'w') as f:
            json.dump(query_dict, f, indent=4)
        if (os.path.exists('output/query_dict_temp.json')):
            os.remove('output/query_dict_temp.json')
    return new_id


def import_data(query_path):
    with open(query_path, 'r') as f:
        query = f.read().replace("\n", " ")
    conn = psycopg2.connect(dbname="",
                            user=input("Username: "),
                            password=getpass('Password (will be hidden and not stored): ', stream = sys.stderr),
                            host="",
                            port="",
                            sslmode="require")
    print('Importing data...')
    output = pd.read_sql(query, conn)
    conn.close()
    spec_name = gen_spec_name(query)
    output.to_csv('raw_data/data_spec={0}.csv'.format(spec_name), index=False)
    print('Data has query id {0} and is located in the subdirectory raw_data'.format(spec_name))
    return spec_name
