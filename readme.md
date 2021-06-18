# fastcluster+

## About
fastcluster+ is an interface to perform complex fuzzy matching on cross-sectional datasets that contain numerical and categorical data. In essence, it provides a project template and wrapper functions optimized to perform comprehensive, large-scale fuzzy matching using the methods provided by the ``fastcluster`` library ([link](https://github.com/dmuellner/fastcluster)).

The raw data can be imported either as a ``.csv`` or queried directly from a SQL server (developed + tested on PostgreSQL).

The program was designed for the following scenario. Suppose you have a cross-sectional dataset where each row represents a record submitted by a person through a unique organization. The data does not include person ID as a field, and the person may have provided marginally different responses across records. Considering this, you would like to identify records submitted by the same person. For example, a row may represent the application to university *j* for person *i*. Person *i* may have submitted an application to several universities. The goal is to identify each person in this dataset.

At a high level, the program divides the dataset into partitions. Hierarchical agglomerative clustering is performed on each partition to generate "groups", which correspond to people in the example above. Users have partial control over the homogeneity of groups by adjusting ϵ (referred to as ``eps``). Increasing ϵ relaxes the definition of what constitutes a group. The algorithm does not force groups to contain multiple records. In the context of the example above, this would correspond to someone who only applied to a single university.

Details on the methodology of the hierarchical agglomerative clustering implemented can be found in ``methodology.pdf``.

## How to run
Perform the following steps to generate the set of grouped objects (details to follow)

1. Populate ``config.json`` with the desired configuration
2. Place raw data in ``raw_data`` subdirectory
3. Modify ``clean_data.py`` to process raw data as desired
4. In the command line, run ``main.py`` which takes the following arguments
  - ``--config=[]``, the name of the config file (``config.json``)
  - ``--query=[]`` (optional), the relative path to the file containing the query to execute
  - ``--data=[]`` (optional), the relative path to the raw data file
  - ``--nworkers=[]`` (optional), number of workers used in multiprocessing step. Default is 1.

__Populate config file__

The config file is the instruction set the program reads to determine which variables to partition and cluster on as well as which values of ϵ to use in generating groups.

Below is a skeleton ``config.json``
```
{
"tol_dict": {
    "var1": {
        "col_name":  "",
        "weight": ""
    }
},
"eps": [],
"cluster_method": "fastcluster",
"partition_vars": [],
"datevar": ""
}
```

- ``tol_dict`` is a dictionary of dictionaries, where each child dictionary represents the variables used to perform within-partition clustering as well as their properties. ``var1`` refers to the label the user assigns to a given variable. ``col_name`` is the actual name of the variable in the dataset. ``weight`` refers to the relative weight assigned to a tolerance variable. That is, if there are two tolerance variables ``var1`` and ``var2`` with weights 1 and 2, respectively, then in determining whether two records should be associated with the same person, the difference between the two records across the ``var2`` dimension will have a penalty double that of the difference between the two records across the ``var1`` dimension. In other words, the algorithm will be less tolerant of differences across ``var2`` than of ``var1``. Users can set the weight to be either a scalar or a lambda function. In the case of a lambda function, the variable represents the difference between two records across the associated tolerance variable. For example, suppose we have a variable where if the difference of two of its recorded values is less than 15, then treat it as if there is no difference. This would be expressed as ``"weight": "lambda x: 0 if x < 15 else 1"``.

- ``eps`` maps to the list of ϵ values (of type ``int``) to use in generating groups. If ``eps=x``, then for any group, no two records in the group differ by more than ``x``.
- ``cluster_method`` refers to the type of clustering performed. Currently only agglomerative hierarchical clustering via the ``fastcluster`` library is available. Thus, this should always be set to ``fastcluster``.
- ``partition_vars`` is a list of variable names used to partition the dataset. These variables can be both numerical and categorical.
- ``datevar`` is the name of the variable that corresponds to a date (e.g., date the record was submitted). If there is no date variable, the value should remain an empty string.

__Read in raw data__
If the raw dataset is stored locally as a ``.csv``, this step is simple. Move the file to the ``raw_data`` subdirectory.

Otherwise, if you wish to query data from a SQL server you first need to write the query. __Note that the program is compatible only with PostgreSQL servers__. Create a new ``.txt`` file and write the query in the form of
```
SELECT
  variables, to, select
FROM
  table.to.query
WHERE
  var1 < i AND
  ... AND
  varX < j
LIMIT k /*optional*/
```
``JOIN`` and ``CASE...WHEN...THEN`` are valid operations. ``GROUP BY`` operations are not valid except within subqueries.

After writing the query, the user must specify the database they wish to access. This is done in ``import_data.py`` within the following code block
```
conn = psycopg2.connect(dbname="",
                        user=input("Username: "),
                        password=getpass('Password (will be hidden and not stored): ', stream = sys.stderr),
                        host="",
                        port="",
                        sslmode="require")
```

__Generate clean data__

Provided in this repository is the script ``clean_data.py`` which is intended to perform any preprocessing steps on the raw data prior to executing the fuzzy match. By default ``clean_data.py`` has a single function ``clean_data`` which takes the raw dataset and saves it in the ``clean_data`` subdirectory without any modifications. It is up to the user to make modifications to this script as they see fit.

__Perform fuzzy match__

Once you have filled out ``config.json`` and made any modifications to ``clean_data.py``, you can begin the fuzzy match process. To do so, from the command line ``cd`` into the main directory and follow step (4) in the "How to run" section. If you are pulling raw data from a SQL server, you need to specify the ``--config`` and ``--query`` arguments. Otherwise, you need to specify the ``-config`` and ``--data`` arguments.


## Interpreting output
After running ``main.py`` for the first time, up to three files are created and saved in the ``output`` subdirectory: ``spec_dict.json``, ``query_dict.json`` and a file of the form ``groups_df_spec=X.csv``.

``spec_dict.json`` is a dictionary of every specification run on the fuzzy matching algorithm. The keys are the ``spec_id``s, the randomly-generated numbers used to identify different configurations of the model. The value pairings are dictionaries that contain information about the configuration.

``query_dict.json`` is a dictionary of all queries executed, each one mapped to a randomly-generated number. This number is referred to as ``query_spec`` in the configuration details of an entry in ``spec_dict.json``. This file is only generated if the user is pulling in raw data from a SQL server.

``groups_df_spec=X.csv`` is the output ``.csv`` which contains every column in the clean dataset in addition to the columns ``group_id_eps_i`` and ``eps_i_max_dist``, where *i* is a value of ϵ. ``group_id_eps_i`` provides the label of the group with which the record is associated. A label of ``-1`` indicates the record is not associated with any other records. ``eps_i_max_dist`` gives the furthest distance between any two points in the group associated with the record (distance defined by a weighted ℓ-2 norm).
