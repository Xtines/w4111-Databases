
# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
import logging
import os
import json
import pandas as pd
from collections import OrderedDict
# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


# a test for load function
def t_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))


# a test for find_by_template function:
def t_find_by_template():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID','AB', 'H', 'HR', 'RBI'] # a "project" operation by selection a set of columns; equivalent to the SELECT clause in SQL
    tmp = {'teamID':'BOS', 'yearID':'1960'} # this is a "simplified template" language, effectively the WHERE clause in SQL

    csv_tbl = CSVDataTable("batting", connect_info, key_columns= key_cols)
    res = csv_tbl.find_by_template(template=tmp, field_list=fields)

    print("Query result = \n", json.dumps(res, indent=2)) #prints out in json.dump format


# Video_OH_Sep_2019: 36min
def t_find_by_pk():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID','AB', 'H', 'HR', 'RBI'] # a "project" operation by selection a set of columns; equivalent to the SELECT clause in SQL
    key_vals = ['willite01', 'BOS', '1960', '1']

    csv_tbl = CSVDataTable("batting", connect_info, key_columns= key_cols)
    res = csv_tbl.find_by_primary_key(key_vals)

    print("Primary key returned result = \n", json.dumps(res, indent=2))


# #def test_match():
# # a static method which won't need the table's connect_info as in t_load()
# # example from Lec 2 demo: 0h: 49min
#     row = {"cool": 'yes', "db": 'no'}
#     t = {"cool": 'yes'}
#     result = CSVDataTable.matches_template(row, t)
#     print(result)


# Video_OH_Sep_2019: 28min
def test_matches():

    r = {
        "playerID": "webstra01",
        "yearID": "1960",
        "teamID": "BOS",
            "AB": "3",
            "H": "0",
            "HR": "0",
            "RBI": "1"
        }
    tmp = {"playerID": "webstra01"}

    test = CSVDataTable.matches_template(r, tmp)
    print("Test whether row matches template:", test)


# test function demo in Lec 2: 0h: 52m
def test_match_all():
# in this function, we will need the connect_info dict {}

    tmp = {"nameLast": 'Williams', "nameFirst": 'Ted'}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    t = {"cool": 'yes'}
    result = csv_tbl.find_by_template(tmp)
    print(json.dumps(result, indent=2))


# a function to test "Project or SELECT" operation
def test_project():

    r = {
        "playerID": "webstra01",
        "yearID": "1960",
        "teamID": "BOS",
        "AB": "3",
        "H": "0",
        "HR": "0",
        "RBI": "1"
        }
    fields = ["playerID", "yearID", "HR"]

    test = CSVDataTable.project(r, fields)
    print("Test for whether Project func returns matches:", test)


def t_key_to_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tb1 = CSVDataTable("people", connect_info, key_columns=['playerID'])
    k = csv_tb1.key_to_template(['willite01'])
    print(k)

## Calling the test functions

#t_load()

#test_match_all()
#test_matches() #outputs - "Test whether row matches template: True"

#t_find_by_template()

#test_project()

#t_find_by_pk()
#t_key_to_template()

# sandbox text for CSVDataTable update_by_template()
cols = ['playerID', 'birthYear', 'birthMonth', 'birthDay', 'birthCountry', 'birthState', 'birthCity', 'nameFirst',
            'nameLast']
values = ['aardsda01', '1981', '12', '27', 'USA', 'CO', 'Denver', 'David', 'Aardsma']
record_to_update = dict(zip(cols, values))
print(type(record_to_update))
print(record_to_update)

cols2 = ['playerID', 'birthYear', 'birthMonth', 'birthDay', 'birthCountry', 'birthState', 'birthCity', 'nameFirst',
            'nameLast']
values2 = ['aardsda01', '1982', '12', '28', 'USA', 'MI', 'Detroit', 'Kevin', 'Aardsma']
#values3= ['whiterabbit', '1982', '11', '16', 'Canada', 'ON', 'Toronto', 'White', 'Rabbit']
new_values_dict = dict(zip(cols2, values2))

print("\n", "new_values_dict : ", new_values_dict, "\n")
print("Type for new_values_dict", type(new_values_dict), "\n")

df2 = pd.DataFrame(new_values_dict, columns = list(new_values_dict.keys()), index=[0]) # pd.dataframe error: ValueError: If using all scalar values, you must pass an index
print(df2.head())
print("Shape of new values df2: ", df2.shape)

key_cols = ['playerID']
# dictionary.get("bogus", default_value)
rows_pkeys = record_to_update.get(key_cols[0], "Key not returned")
print(rows_pkeys)
print(type(rows_pkeys)) # <class 'str'>

connect_info = {
    "directory": data_dir,
    "file_name": "People-smallcopy.csv"
    }
csv_tbl_update = CSVDataTable("People-smallcopy", connect_info, key_columns=['playerID'])
print("Created table = " + str(csv_tbl_update))
print("type of csv_tbl_update._rows :", type(csv_tbl_update._rows[1]), "\n") # <class 'list'> -> <class 'collections.OrderedDict'>
print("Show csv_tbl_update._rows: ", csv_tbl_update._rows, "\n")

df1 = pd.DataFrame(csv_tbl_update._rows, columns = csv_tbl_update._data["table_columns"]) # hard-coded index column: index=['playerID']
print("Shape of loaded table df1: ", df1.shape)
print(df1.head())

# testing _update_row(self, row_to_update, new_values_dict):
# df.reset_index(inplace=True) -- "name" column becomes a column and the new index is the standard default integer index: # https://stackoverflow.com/questions/49720616/python-pandas-dataframe-set-index-how-to-keep-the-old-index-column
df1.set_index(key_cols, inplace=True)
df1.update(df2.set_index(key_cols))
df1.reset_index(inplace=True)
print("Updated table containing updated row: ", "\n", df1, "\n")

# write back updated table df to self._rows or csv_tbl_update._rows:
## need to convert df into a list of OrderedDicts
#>>> [df.to_dict(orient='index')]
#[{0: {'col1': 1, 'col2': 3}, 1: {'col1': 2, 'col2': 4}}]
#>>> df.to_dict(orient='records')
#[{'col1': 1, 'col2': 3}, {'col1': 2, 'col2': 4}]

# wrong format: print("df1 to dict conversion: ", df1.to_dict(orient='records'))  # [{'birthYear': '1982', 'birthMonth': '12', 'birthDay': '28', 'birthCountry': 'USA' ...
from collections import OrderedDict
csv_tbl_update._rows = df1.to_dict(into=OrderedDict, orient='records')
print ("Updated row results:", json.dumps(csv_tbl_update._rows, indent=2))

tmptest1 = record_to_update # original row before update
res = csv_tbl_update.find_by_template(template=tmptest1, field_list=None)

print("\n", "Test that update method worked: Re-executing our find_by_template() method should return no rows:,", "\n", json.dumps(res, indent=2))