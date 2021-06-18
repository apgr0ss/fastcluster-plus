import pandas as pd
import numpy as np
import os

def clean_data(raw_data_path, query_id = None):
    """
    Clean raw data. Defaults to returning raw_data with no modifications.
    User can modify this file as needed.
    """
    raw_data = pd.read_csv(raw_data_path)
    data = raw_data
    if query_id is not None:
        data.to_csv('clean_data/data_spec={0}.csv'.format(query_id), index=False)
    else:
        filename = os.path.split(raw_data_path)[-1]
        data.to_csv('clean_data/' + filename, index=False)
