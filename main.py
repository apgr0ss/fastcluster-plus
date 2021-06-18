import argparse
import json
import re
import os
from import_data import import_data
from clean_data import clean_data
from cluster import gen_groups

"""
Main module to execute group generation. Call from command line.
"""

def query_setify(query):
    items_select = re.findall(r'(?<=SELECT)(.*)(?=FROM)', query)[0]
    items_select_list = items_select.split(',')
    items_select_list = [item.strip() for item in items_select_list]

    item_from = re.findall(r'(?<=FROM)(.*)(?=WHERE)', query)[0]
    item_from = item_from.strip()

    items_where = re.findall(r'(?<=WHERE)(.*)', query)[0]
    items_where_list = items_where.split('AND')
    items_where_list = [item.strip() for item in items_where_list]

    return (set(items_select_list), set([item_from]), set(items_where_list))


def check_query(query_path):
    """
    Check if query has already been processed
    If processed, return query id
    If not processed, return None
    """

    with open(query_path, 'r') as f:
        q_to_run = f.read()
    if (not os.path.isfile('output/query_dict.json')):
        return None
    with open('output/query_dict.json', 'r') as f:
        q_dict = json.load(f)

    q_remove_chars = q_to_run.replace('\n', ' ')
    q_remove_chars = q_remove_chars.replace('\t', ' ')
    query_id = None
    q_to_run_select, q_to_run_from, q_to_run_where = query_setify(q_remove_chars)
    for key, q in q_dict.items():
        q_select, q_from, q_where = query_setify(q)
        if (q_to_run_select == q_select and q_to_run_from == q_from and q_to_run_where == q_where):
            query_id = int(key)
    return query_id

if __name__ == "__main__":
    # Collect argument(s)
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str)
    parser.add_argument('--query', nargs='?', type=str)
    parser.add_argument('--data', nargs='?', type=str)
    parser.add_argument('--nworkers', nargs='?', default=1, type=int)
    args = parser.parse_args()

    config_path = args.config
    query_path  = args.query
    data_path   = args.data
    n_workers   = args.nworkers

    with open(config_path, 'r') as f:
        config = json.load(f)

    if query_path is not None and data_path is not None:
        assert False, "Cannot specify both query and data path"
    elif query_path is not None:
        query_id = check_query(query_path)
        if (query_id is None):
            print('Data has not been queried before. Data must be imported.')
            print('Enter credentials to query data')
            query_id = import_data(query_path)
            clean_data('raw_data/data_spec={0}'.format(query_id), query_id)
            print('Clean data saved to clean_data subdirectory with query id {0}'.format(query_id))
        config['query_spec'] = query_id
        gen_groups(config, n_workers)
    else:
        if not os.path.isfile(data_path):
            assert False, 'Path to data is not valid'
        config['query_spec'] = None
        clean_data(data_path)
        gen_groups(config, n_workers, data_path)
