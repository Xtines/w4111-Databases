from src.RDBDataTable import RDBDataTable

import pymysql
import copy
import csv
import json
import os
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# RDB test 1: for establishing a connection with MySQL database and retreiving sample rows from a given table;
def test1_RDB_MySQL_connection():
    print("\n\n")
    print("******************** " + "START test1_MySQL_connection" + " ********************")
    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }

    r_dbt = RDBDataTable("appearances", connect_info=c_info, key_columns=['playerID', 'teamID', 'yearID'])
    print("RDB table = ", r_dbt)
    print("******************** " + "END test1_MySQL_connection" + " ********************")
    print("\n\n")

#test1_RDB_MySQL_connection()


# RDB test 2:
def test_run_q():
    print("\n\n")
    print("******************** " + "START test_run_q" + " ********************")

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    RDB_tb1 = RDBDataTable("appearances", connect_info=c_info, key_columns=['playerID', 'teamID', 'yearID'])

    # SELECT * FROM appearances WHERE teamID = 'BOS' AND yearID = '1950'
    query = "SELECT * FROM appearances WHERE teamID =%s AND yearID =%s"
    print("We are testing this SQL query command: ", query, " with args as: ('BOS', '1950')", "\n\n")
    result = RDB_tb1._run_q(query, args=('BOS', '1950'))

    print("SQL query result: ", "\n\n",
          json.dumps(result, indent=2), "\n\n")

    print("*******************  " + "END test_run_q" + " ********************")
    print("\n\n")

#test_run_q()


# RDB test 3:
#find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None, commit=True)
def test_RDB_find_by_template():
    print("\n\n")
    print("******************** " + "START test_RDB_find_by_template" + " ********************")

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    RDB_tb1 = RDBDataTable("appearances", connect_info=c_info, key_columns=['playerID', 'teamID', 'yearID'])

    correct_sol = ['1953', 'BOS', 'AL', 'willite01', '37', '26', '37', '26', '0', '0', '0', '0', '0', '0', '26', '0', '0', '26', '0', '11', '0']

    tmp = {'playerID': 'willite01', 'teamID':'BOS', 'yearID':'1953'} # this is a "simplified template" language, effectively the WHERE clause in SQL
    res = RDB_tb1.find_by_template(template=tmp, field_list= None)

    print("If the method is successful, we expect to see one record returned with the following field values: ", "\n\n", correct_sol, "\n\n")
    print("Find_by_template method result =", "\n\n", json.dumps(res, indent=2))

    print("******************** " + "END test_RDB_find_by_template" + " ********************")
    print("\n\n")

#test_RDB_find_by_template()


# RDB test 4:
def test_RDB_delete_by_template():
    print("\n\n")
    print("******************** " + "START test_RDB_delete_by_template" + " ********************")

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    RDB_tb1 = RDBDataTable("appearances", connect_info=c_info, key_columns=['playerID', 'teamID', 'yearID'])



    print("******************** " + "END test_RDB_delete_by_template" + " ********************")
    print("\n\n")


# RDB test 5:
def test_RDB_update_by_template():
    print("\n\n")
    print("******************** " + "START test_RDB_update_by_template" + " ********************")

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    RDB_tb1 = RDBDataTable("appearances", connect_info=c_info, key_columns=['playerID', 'teamID', 'yearID'])

    tmp_toupdate = {'playerID':'ruthba01', 'teamID': 'NYA', 'yearID':'1930'}
    new_values = {"G_all": "146"}
    result_update_bytmp = RDB_tb1.update_by_template(template=tmp_toupdate, new_values=new_values)
    print(result_update_bytmp)

    # check updated row result:
    checktemplate = {'playerID':'ruthba01', 'teamID': 'NYA', 'yearID':'1930'}
    check_updated_by_tmp = RDB_tb1.find_by_template(template=checktemplate, field_list= None)

    print("Inspect updated record which should contain the updated value for G_all of 146, from 145: ",
          "\n\n", json.dumps(check_updated_by_tmp, indent=2))

    print("******************** " + "END test_RDB_update_by_template" + " ********************")
    print("\n\n")


test_RDB_update_by_template()


# RDB test 6:
def test_RDB_insert():
    print("\n\n")
    print("******************** " + "START test_RDB_update_by_template" + " ********************")

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    RDB_tb1 = RDBDataTable("appearances", connect_info=c_info, key_columns=['playerID', 'teamID', 'yearID'])



    print("******************** " + "END test_RDB_update_by_template" + " ********************")
    print("\n\n")


#######-------------- instantiating RDB table object to call test functions ------------############

#test1_RDB_MySQL_connection()

#test_run_q()

#test_RDB_find_by_template()