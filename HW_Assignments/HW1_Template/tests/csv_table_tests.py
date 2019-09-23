
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
def test_load():
    print("\n\n")
    print("******************** " + "START test_load" + " ********************")
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))
    print("******************** " + "END test_load" + " ********************")
    print("\n\n")


# a test for find_by_template() function:
def test_find_by_template():
    print("\n\n")
    print("******************** " + "START test_find_by_template" + " ********************")
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID','AB', 'H', 'HR', 'RBI'] # a "project" operation by selection a set of columns; equivalent to the SELECT clause in SQL
    tmp = {'teamID':'BOS', 'yearID':'1960', 'stint': '2'} # this is a "simplified template" language, effectively the WHERE clause in SQL

    csv_tbl = CSVDataTable("batting", connect_info, key_columns= key_cols)
    res = csv_tbl.find_by_template(template=tmp, field_list=fields)

    print("Query result = \n", json.dumps(res, indent=2)) #prints out in json.dump format

    print("******************** " + "END test_find_by_template" + " ********************")
    print("\n\n")

# Video_OH_Sep_2019: 36min

def test_find_by_pk():
    print("\n\n")
    print("******************** " + "START test_find_by_pk" + " ********************")
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
    print("******************** " + "END test_find_by_pk" + " ********************")
    print("\n\n")

# #def test_match():
# # a static method which won't need the table's connect_info as in t_load()
# # example from Lec 2 demo: 0h: 49min
#     row = {"cool": 'yes', "db": 'no'}
#     t = {"cool": 'yes'}
#     result = CSVDataTable.matches_template(row, t)
#     print(result)


# Video_OH_Sep_2019: 28min
def test_matches():
    print("\n\n")
    print("******************** " + "START test_matches" + " ********************")
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
    print("******************** " + "END test_matches" + " ********************")
    print("\n\n")


# test function demo in Lec 2: 0h: 52m
def test_match_all():
# in this function, we will need the connect_info dict {}
    print("\n\n")
    print("******************** " + "START test_match_all" + " ********************")

    tmp = {"nameLast": 'Williams', "nameFirst": 'Ted'}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    #t = {"cool": 'yes'}
    result = csv_tbl.find_by_template(tmp)
    print("Matched records ", "\n", json.dumps(result, indent=2), "\n\n")

    print("******************** " + "END test_match_all" + " ********************")
    print("\n\n")


# a function to test "Project or SELECT" operation
def test_project_operation():
    print("\n\n")
    print("******************** " + "START test_project_operation" + " ********************")

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
    print("Test for whether Project func returns matches:", test, "\n\n")
    print("******************** " + "END test_project_operation" + " ********************")
    print("\n\n")


def test_key_to_template():
    print("\n\n")
    print("******************** " + "START test_key_to_template" + " ********************")
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tb1 = CSVDataTable("people", connect_info, key_columns=['playerID'])
    k = csv_tb1.key_to_template(['willite01'])
    print("result of key_to_template():", k, "\n\n", "length of k:", len(k), "\n\n")

    print("******************** " + "END test_key_to_template" + " ********************")
    print("\n\n")


#delete_by_template(self, template)
def test_delete_by_template():
    print("\n\n")
    print("******************** " + "START test_delete_by_template" + " ********************")
    connect_info = {
        "directory": data_dir,
        "file_name": "People-smallcopy.csv"
    }
    csv_tb3 = CSVDataTable("People-smallcopy", connect_info, key_columns=['playerID'])
    no_rows_pre = len(csv_tb3._rows)
    print("no. of rows before operation: ", no_rows_pre, "\n")

    tmp = {'playerID': 'aardsda01'}
    result = csv_tb3.delete_by_template(tmp)
    no_rows_post = len(csv_tb3._rows)
    print("no. of rows after operation: ", no_rows_post, "\n")

    #Executing a find that demonstrates the changed data.
    res = csv_tb3.find_by_template(template=tmp, field_list=None)
    print("re-executing our find_by_template() method should return no rows:,", "\n", json.dumps(res, indent=2))

    print("******************** " + "END test_delete_by_template" + " ********************")
    print("\n\n")
    #print("result of key_to_template():", result, "\n", "length of k:", len(result)) -> error: TypeError: object of type 'NoneType' has no len()


def test_update_by_template():
    print("\n\n")
    print("******************** " + "START test_update_by_template" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "People-smallcopy.csv"
    }
    key_cols = ['playerID']
    csv_tb3 = CSVDataTable("People-smallcopy", connect_info, key_columns= key_cols)

    # create a template for updating:
    cols = ['playerID', 'birthYear', 'birthMonth', 'birthDay', 'birthCountry', 'birthState', 'birthCity', 'nameFirst', 'nameLast']
    values = ['aardsda01', '1981', '12', '27', 'USA', 'CO', 'Denver', 'David', 'Aardsma']
    original_record_tmp = dict(zip(cols, values))

    print("Show original record tmp: ", original_record_tmp, "\n\n")

    #tmp = {'playerID': 'aardsda01'}
    new_values_dict = {'playerID': 'aardsda01', 'birthYear': '1982', 'birthMonth': '12', 'birthDay': '28', 'birthCountry': 'USA', 'birthState': 'MI', 'birthCity': 'Detroit', 'nameFirst': 'Kevin', 'nameLast': 'MickyMouse'}
    print("Show new_values_dict for updating: ", new_values_dict, "\n\n")

    #update_by_template(self, template, new_values_dict)
    csv_tb3.update_by_template(original_record_tmp, new_values_dict)

    #Executing a find that demonstrates the changed data.
    res = csv_tb3.find_by_template(template=original_record_tmp, field_list=None)
    print("re-executing our find_by_template() for the original record should show no results:,", "\n", json.dumps(res, indent=2))

    print("******************** " + "END test_update_by_template" + " ********************")
    print("\n\n")



### Calling all of the above test functions -------------------------------

#test_load()
test_load()

#test_match_all()
test_match_all()

test_matches() #outputs - "Test whether row matches template: True"

#test_find_by_template()
test_find_by_template()

#test_project_operation
test_project_operation

#test_find_by_pk()
test_find_by_pk()

#test_key_to_template()
test_key_to_template()

#test_delete_by_template()
test_delete_by_template()

#test_update_by_template()
test_update_by_template()