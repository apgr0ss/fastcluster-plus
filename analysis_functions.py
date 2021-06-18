import random
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
import fastcluster as fc
import dask.array as da
import datetime

class DendrogramNode:
    def __init__(self, node_size, max_dist, node_id, members=[]):
        self.node_size = node_size
        self.max_dist = max_dist
        self.node_id = node_id
        self.members = members
        self.left_child = None
        self.right_child = None

    def has_children(self):
        return self.left_child is not None and self.right_child is not None


def build_tree(d_list, N, node_id="1"):
    """
    Construct binary tree from fastcluster output
    """
    if len(d_list) < 1:
        return None
    else:
        last = d_list[-1]
        node = DendrogramNode(node_size=int(last[-1]),
                              max_dist=last[-2],
                              node_id=node_id)
        if last[0] < N:
            # Index in last[0] represents actual data
            left_child = DendrogramNode(node_size=1,
                                        max_dist=0,
                                        members=[int(last[0])],
                                        node_id=node.node_id + "1")
        else:
            # Index in last[0] represents higher branch
            # Map index to agglomerate step in d_list
            index_map = int(last[0] - N)
            left_child = build_tree(d_list[:(index_map + 1)], N, node_id=node_id + "1")

        # Perform same procedure for right child
        if last[1] < N:
            right_child = DendrogramNode(node_size=1,
                                         max_dist=0,
                                         members=[int(last[1])],
                                         node_id=node.node_id + "2")
        else:
            index_map = int(last[1] - N)
            right_child = build_tree(d_list[:(index_map + 1)], N, node_id=node_id + "2")

        node.left_child = left_child
        node.right_child = right_child
        node.members = left_child.members + right_child.members

        return node


def assign_groups_recurse(tree, eps, group_labels, group_max_dist):
    if tree.max_dist <= eps:
        if tree.node_size > 1:
            group_labels[tree.members] = tree.node_id
            group_max_dist[tree.members] = tree.max_dist
        else:
            group_labels[tree.members] = "-1"
            group_max_dist[tree.members] = 0
        return (group_labels, group_max_dist)
    else:
        group_labels, group_max_dist = assign_groups_recurse(tree.left_child, eps, group_labels, group_max_dist)
        group_labels, group_max_dist = assign_groups_recurse(tree.right_child, eps, group_labels, group_max_dist)
        return (group_labels, group_max_dist)


def assign_groups(tree, eps):
    n_obs = tree.node_size
    group_labels   = np.repeat("", n_obs).astype(object)
    group_max_dist = np.repeat(0,  n_obs).astype(float)
    return assign_groups_recurse(tree, eps, group_labels, group_max_dist)

def weight(arg_list, fun_list):
    """
    Weight kernel for fastcluster
    Pass in list of weight functions (or ints) and apply elementwise
    to each difference in arg_list

    Return weighted differences
    """
    out = []
    for arg, fun in zip(arg_list, fun_list):
        if type(fun) == int or type(fun) == float:
            out += [fun*arg]
        else:
            fun = eval(fun)
            out += [fun(arg)*arg]
    return np.array(out)


def make_groups_fastcluster(partition, partition_num, partition_len, datevar, eps_vec, tol_dict):
    if len(datevar) > 0:
        timedelta = (pd.to_datetime(partition[datevar]) - datetime.datetime(1970, 1, 1)).dt.days
        partition[datevar] = timedelta.values
    fun_list = [tol_dict[tol_var]['weight'] for tol_var in tol_dict.keys()]
    out = fc.linkage(partition.values, method='complete', metric = lambda u, v: np.sqrt(np.sum(weight(np.abs(u-v), fun_list)*weight(np.abs(u-v), fun_list))))
    tree = build_tree(out, N=partition_len)
    labels_mat = np.ndarray(shape=(len(partition), len(eps_vec)), dtype=str).astype(object)
    max_dist_mat = np.ndarray(shape=(len(partition), len(eps_vec)), dtype=float)
    for i, eps in enumerate(eps_vec):
        labels, max_dist = assign_groups(tree, eps)
        labels_formatted = [str(partition_num) + '@' + label if label != '-1' else str(label) for label in labels]
        labels = labels_formatted
        labels_mat[:, i] = labels
        max_dist_mat[:, i] = max_dist
    return (labels_mat, max_dist_mat)


def make_groups(arg_list):
    partition, partition_num, tol_dict, datevar, cluster_method, eps_vec = arg_list
    np.random.seed(5238)
    n_iters = 1
    labels = None
    for i in range(n_iters):
        if len(partition) == 1:
            labels = ['-1']*len(eps_vec)
            max_dist = [0]*len(eps_vec)
        else:
            partition = partition.iloc[np.random.permutation(range(len(partition))), :]
            partition_subset = partition.loc[:, [tol_dict[key]['col_name'] for key in tol_dict.keys()]]
            partition_len = len(partition)
            labels, max_dist = make_groups_fastcluster(partition_subset, partition_num, partition_len, datevar, eps_vec, tol_dict)
    # Add group labels
    partition.loc[:, ['group_id_eps_{0}'.format(int(eps)) for eps in eps_vec]] = labels
    partition.loc[:, ['group_id_eps_{0}'.format(int(eps)) for eps in eps_vec]] = partition.loc[:, ['group_id_eps_{0}'.format(int(eps)) for eps in eps_vec]].astype(str)
    if cluster_method == 'fastcluster':
        partition.loc[:, ['eps_{0}_max_dist'.format(int(eps)) for eps in eps_vec]] = max_dist
    partition = partition.sort_values('internal_order')
    return (partition.loc[:, ['group_id_eps_{0}'.format(eps) for eps in eps_vec]].values,
            partition.loc[:, ['eps_{0}_max_dist'.format(eps) for eps in eps_vec]].values)
