import pandas as pd
import itertools
from analysis_functions import *
import os
import re
import json
import datetime
import copy
import time
import tqdm
import dill
from multiprocessing import Pool


def spec_exists(spec_dict, new_entry):
    return_key = None
    spec_dict_dup = copy.deepcopy(spec_dict)
    new_entry_dup = copy.deepcopy(new_entry)
    new_entry_dup.pop('date_created')
    for key, spec in spec_dict_dup.items():
        spec.pop('date_created')
        if spec == new_entry_dup:
            return_key = key
    return return_key


def gen_file_name(tol_dict, data, eps, cluster_method, partition_vars, query_spec, datevar):
    if os.path.exists('output/spec_dict.json'):
        with open('output/spec_dict.json', 'r') as f:
            spec_dict = json.load(f)
        with open('output/spec_dict_temp.json', 'w') as f:
            json.dump(spec_dict, f, indent=4)
    else:
        spec_dict = {}

    val = {'date_created': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
           'tol_dict': tol_dict,
           'eps_min': np.min([float(eps_val) for eps_val in eps]),
           'eps_max': np.max([float(eps_val) for eps_val in eps]),
           'cluster_method': cluster_method,
           'partition_vars': partition_vars,
           'datevar': datevar,
           'query_spec': query_spec
           }

    new_id = spec_exists(spec_dict, val) if len(spec_dict.keys()) > 0 else None
    if new_id is None:
        ids = list(spec_dict.keys())
        new_id = np.random.randint(10000)
        while new_id in ids:
            new_id = np.random.randint(10000)
        spec_dict[new_id] = val
        with open('output/spec_dict.json', 'w') as f:
            json.dump(spec_dict, f, indent=4)
        if os.path.exists('output/spec_dict_temp.json'):
            os.remove('output/spec_dict_temp.json')
    return new_id


def gen_key_dict(gb_keys):
    return {key: i for (i, key) in enumerate(gb_keys)}


def is_tup_match(tup_1, tup_2):
    return all([item_1 == item_2 for item_1, item_2 in zip(tup_1, tup_2)])


def get_partition_number(x, gb_keys):
    return np.where([is_tup_match(x.name, key) for key in gb_keys])[0][0]

def applyParallel(df, func, tol_dict, datevar, cluster_method, eps_vec, n_workers):
    groups = df.groupby('partition_label')
    with Pool(n_workers) as p:
        arg_list = [(group, name, tol_dict, datevar, cluster_method, eps_vec) for (name,group) in groups]
        ret_list = list(tqdm.tqdm(p.imap(func, arg_list), total=len(arg_list)))
    print('Combining labels for specified epsilon values...')
    df_sorted = df.sort_values(['partition_label','internal_order'])
    last_idx = 0
    group_list = np.empty((len(df_sorted), len(eps_vec)), dtype=str).astype(object)
    max_dist_list = np.zeros((len(df_sorted), len(eps_vec))).astype(float)

    for label_tup in ret_list:
        group_list[last_idx:(last_idx+len(label_tup[0])), :] = label_tup[0]
        max_dist_list[last_idx:(last_idx+len(label_tup[1])), :] = label_tup[1]
        last_idx = last_idx + len(label_tup[0])
    df_sorted.loc[:, ['group_id_eps_{0}'.format(eps) for eps in eps_vec]] = group_list
    df_sorted.loc[:, ['eps_{0}_max_dist'.format(eps) for eps in eps_vec]] = max_dist_list
    return df_sorted


def fuzzy_iteration(filename_dir, tol_dict, eps_vec, cluster_method, partition_vars, datevar, query_spec, n_workers):
    print('Reading in data...')
    data = pd.read_csv(filename_dir)
    data['internal_order'] = list(range(len(data)))
    print('Grouping by partition variables')
    data_grouped = data.groupby(partition_vars)
    print('Generating key dictionary...')
    df_names = data_grouped.apply(lambda x: x.name).values
    key_dict = gen_key_dict(df_names)
    print('Assigning partition labels to observations...')
    data['partition_label'] = data.apply(lambda x: key_dict[tuple(x[var] for var in partition_vars)], axis=1)
    print('Creating groups with specified epsilon values...')
    for eps in eps_vec:
        data['group_id_eps_{0}'.format(eps)] = '-1'
        data['eps_{0}_max_dist'.format(eps)] = 0.0
    data = applyParallel(data, make_groups, tol_dict, datevar, cluster_method, eps_vec, n_workers)
    data.drop('partition_label', inplace=True, axis=1)
    data.drop('internal_order', inplace=True, axis=1)
    spec_name = gen_file_name(tol_dict, data, eps_vec, cluster_method, partition_vars, query_spec, datevar)
    print('Complete. Saving results as spec {0}.'.format(spec_name))
    data.to_csv('output/' + 'groups_df_spec={0}.csv'.format(spec_name), index=False)



def gen_groups(config, n_workers, data_path = ''):
    query_spec = config['query_spec']
    cluster_method = config['cluster_method']
    epsilon_values = config['eps']
    tolerance_values = config['tol_dict']
    partition_vars = config['partition_vars']
    datevar = config['datevar']
    filename_dir = 'clean_data/data_spec={0}.csv'.format(query_spec) if data_path == '' else data_path
    fuzzy_iteration(filename_dir=filename_dir,
                    tol_dict=tolerance_values,
                    eps_vec=epsilon_values,
                    cluster_method=cluster_method,
                    partition_vars=partition_vars,
                    datevar = datevar,
                    query_spec=query_spec,
                    n_workers = n_workers)
