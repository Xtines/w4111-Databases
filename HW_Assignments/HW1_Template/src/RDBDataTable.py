from src.BaseDataTable import BaseDataTable
import pymysql

import copy
import csv
import json
import os
import pandas as pd
import logging

import logging
logger = logging.getLogger()
import operator

#https://github.com/donald-f-ferguson/w4111-Databases/blob/master/Examples/SQLHelper.py
'''
The default DB connection information. You MUST use this connection information for your assignments
unless we specify otherwise. This means that you will have to:
1. Create the schema lahman2019raw. 
2. 
We will show how to do this.
You MUST create a user dbuser with the password dbuserdbuser.
'''



### above from Help Functions provided by instructor


class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def get_connection(connect_info):
        """

        :param connect_info: A dictionary containing the information necessary to make a PyMySQL connection.
        :return: The connection. May raise an Exception/Error.
        """

        cnx = pymysql.connect(**connect_info)
        return cnx

    #_default_connect_info = {
    #    'host': 'localhost',
    #    'user': 'dbuser',
    #    'password': 'dbuserdbuser',
    #    'db': 'lahman2019raw',
    #    'charset':'utf8mb4',
    #    'port': 3306
    #}

    def _get_default_connection(self):
        result = pymysql.connect(host='localhost',
                                 user='dbuser',
                                 password='dbuserdbuser',
                                 db='lahman2019raw',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
        return result

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        #initialize and store information in class object
        super().__init__(table_name, connect_info, key_columns)
        self._table_name = table_name
        self._connect_info = connect_info
        self._key_columns = key_columns
        self._connectx = self._get_default_connection()
        self._rows = None
        self._column_names = None

        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "row_count": None
        }

        if table_name is None or connect_info is None:
            raise ValueError("Invalid input.")

        # creates an instance variable (a connection) once, so that you don't have to connect everytime?
        cnx = RDBDataTable.get_connection(connect_info)
        if cnx is not None:
            self._cnx = cnx
        else:
            raise Exception("Could not get a connection.")

        self._key_columns = self._get_primarykeys() #in CSVDataTable.py primary keys are stated when table instant. - key_cols = self._data["key_columns"]
        print("Primary key(s) for ", self._data["table_name"], "include: ", self._key_columns)

        #if len(tn) == 2:
        #    self._schema = tn[0]
        #    self._table = tn[1]
        #elif len(tn)== 1:
        #    self._schema = self._connect_info['db']
        #    self._table = tn[0]
        #pass

    def __str__(self):
        result = "RDBDataTable:\n"
        result += json.dumps(self._data, indent=2)

        row_count = self.get_row_count()
        result += "\nNumber of rows = " + str(row_count)

        some_rows = pd.read_sql(
            "select * from " + self._data["table_name"] + " limit 10",
            con=self._cnx
            )
        result += "First 10 rows = \n"
        result += str(some_rows)

        return result

    def get_row_count(self):
        row_count = self._data.get("row_count", None)
        if row_count is None:
            sql = "select count(*) as count from " + self._data["table_name"]
            res, d = self._run_q(sql, args=None, fetch=True, conn=self._cnx, commit=True)
            row_count = d[0][0]
            self._data['row_count'] = row_count

        return row_count


    def _get_primarykeys(self):
        #SHOW KEYS FROM table WHERE Key_name = 'PRIMARY' #https://stackoverflow.com/questions/2341278/how-to-get-primary-key-of-table/2341388

        q1 = "SHOW KEYS FROM " + self._table_name + " WHERE Key_name = 'PRIMARY'"
        rows = self._run_q(q1, args=None, fetch=True)

        print("type of rows[1]", type(rows[1]), '\n',
              "len(rows):", len(rows[1]), '\n',
              "rows:", rows) # "returned Column_name:", rows[1][0]['Column_name']
        list1 = rows[1] # list of 4 items in rows[1] :  (4, [{'Table': 'batting', 'Non_unique': 0, 'Key_name': 'PRIMARY', 'Seq_in_index': 1, 'Column_name': 'teamID', 'Collation': 'A', 'Cardinality': 153, 'Sub_part': None, 'Packed': None, 'Null': '', 'Index_type': 'BTREE', 'Comment': '', 'Index_comment': '', 'Visible': 'YES', 'Expression': None}, {'Table': 'batting', 'Non_unique': 0, 'Key_name': 'PRIMARY', 'Seq_in_index': 2, 'Column_name': 'playerID', 'Collation': 'A', 'Cardinality': 41463,

        keys = []
        for i in list1:
            print("Retrieving primary keys from returned 'Column_name' in db:", i['Column_name'])
            keys.append(i['Column_name'])
        return keys
        #(1, [{'Table': 'people', 'Non_unique': 0, 'Key_name': 'PRIMARY', 'Seq_in_index': 1, 'Column_name': 'playerID', 'Collation': 'A', 'Cardinality': 18984, 'Sub_part': None, 'Packed': None, 'Null': '', 'Index_type': 'BTREE', 'Comment': '', 'Index_comment': '', 'Visible': 'YES', 'Expression': None}])
        #Primary keys: None
        # rows type <class 'tuple'> -> rows type[1] <class 'list'> -> rows type[1][0] <class 'dict'>
        #  len(rows): 15

        #if rows and len(rows) > 0:
        #    keys = [[r['Column_name'], r['Seq_in_index']] for r[1][0] in rows]
        #    keys = sorted(keys, key= operator.itemgetter(1))
        #    keys = [k[0] for k in keys]

        #print("Primary keys according to MySQL database: ", keys)
        #With Python 3, since map returns an iterator, use list to return a list, e.g. list(map(operator.itemgetter('value'), l)).


    def _run_q(self, sql, args=None, fields= None, fetch=True, cur=None, conn=None, commit=True):
        '''
        Helper function to run an SQL statement.
        :param sql: SQL template with placeholders for parameters.
        :param args: Values to pass with statement.
        :param fetch: Execute a fetch and return data.
        :param conn: The database connection to use. The function will use the default if None.
        :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
        :param commit: This is wizard stuff. Do not worry about it.
        :return: A tuple of the form (execute response, fetched data)
        '''

        cursor_created = False
        connection_created = False

        try:

            if conn is None:
                connection_created = True
                conn = self._get_default_connection()

            # inserted below to enable joining of field list in SQL SELECT clause
            if fields:
                sql = sql.format(",".join(fields))

            if cur is None:
                cursor_created = True
                cur = conn.cursor()

            if args is not None:
                log_message = cur.mogrify(sql, args)
            else:
                log_message = sql

            logger.debug("Executing SQL = " + log_message)

            res = cur.execute(sql, args)

            if fetch:
                data = cur.fetchall()
            else:
                data = None

            # Do not ask.
            if commit == True:
                conn.commit()

        except Exception as e:
            raise (e)

        return (res, data)


    def create_select(table_name, template, fields, order_by=None, limit=None, offset=None):
        """
        Produce a select statement: sql string and args.
        :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
        :param fields: Columns to select (an array of column name)
        :param template: One of Don Ferguson's weird JSON/python dictionary templates.
        :param order_by: Ignore for now.
        :param limit: Ignore for now.
        :param offset: Ignore for now.
        :return: A tuple of the form (sql string, args), where the sql string is a template.
        """

        if fields is None:
            field_list = " * "
        else:
            field_list = " " + ",".join(fields) + " "

        w_clause, args = template_to_where_clause(template)

        sql = "select " + field_list + " from " + table_name + " " + w_clause

        return (sql, args)


    def template_to_where_clause(template):
        """
        :param template: One of those weird templates
        :return: WHERE clause corresponding to the template.
        """

        if template is None or template == {}:
            result = (None, None)
        else:
            args = []
            terms = []

            for k, v in template.items():
                terms.append(" " + k + "=%s ")
                args.append(v)

            w_clause = "AND".join(terms)
            w_clause = " WHERE " + w_clause

            result = (w_clause, args)

        return result


    def create_insert(table_name, row, values_tuple): #:param row: A Python dictionary of the form: { ..., "column_name" : value, ...}
        """
        :param table_name: A table name, which may be fully qualified.
        :param row: A Python dictionary of the form: { ..., "column_name" : value, ...}
        :return: SQL template string, args for insertion into the template
        """

        result = "Insert into " + table_name + " "
        cols = row.keys()
        vals = values_tuple #row.values() error - AttributeError: 'dict_values' object has no attribute 'translate'

        # This is paranoia. I know that calling keys() and values() should return in matching order,
        # but in the long term only the paranoid survive.
        #for k, v in row.items():
        #    cols.append(k)
        #    vals.append(v)

        col_clause = "(" + ",".join(cols) + ") " #INSERT INTO books(title,isbn)

        no_cols = len(cols)
        terms = ["%s"] * no_cols
        terms = ",".join(terms)
        value_clause = " values (" + terms + ")"

        result += col_clause + value_clause
        #mySql_insert_query = """INSERT INTO Laptop (Id, Name, Price, Purchase_date)
                                #VALUES (%s, %s, %s, %s) """

        return (result, vals)  #(result, vals) - should the tuple be there?

    #INSERT INTO books(title,isbn)
    #VALUES('Harry Potter And The Order Of The Phoenix', '9780439358071'),
    #  ('Gone with the Wind', '9780446675536'),
    #  ('Pride and Prejudice (Modern Library Classics)', '9780679783268');


    def create_update(table_name, new_values, template):
        """
        :param new_values: A dictionary containing cols and the new values.
        :param template: A template to form the where clause.
        :return: An update statement template and args.
        """
        set_terms = []
        args = []

        for k, v in new_values.items():
            set_terms.append(k + "=%s")
            args.append(v)

        s_clause = ",".join(set_terms)
        w_clause, w_args = RDBDataTable.template_to_where_clause(template)

        # There are %s in the SET clause and the WHERE clause. We need to form
        # the combined args list.
        args.extend(w_args)

        sql = "update " + table_name + " set " + s_clause + " " + w_clause

        return sql, args



######### Helper functions above; HW1 template methods below #################333


    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        pass

#1
    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None, commit=True): #added commit=True
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        where_clause = RDBDataTable.template_to_where_clause(template)
        #print("find_by_template() method's WHERE clause:", where_clause, "\n")

        # we want to SELECT all fields when field_list is not specified:
        if field_list is None:
            fields_selected = ['*']
        else:
            fields_selected = field_list

        q = "SELECT {} from " + self._table_name + " " + where_clause[0]
        print("find_by_template() method's WHERE clause:", q, "\n", "args:", where_clause[1])

        result = self._run_q(q, args=where_clause[1], fields=fields_selected, fetch=True, commit=commit)
        #result = self._project(result, fields_selected)
        return result


    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        pass

#2
    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        try:
            where_clause = RDBDataTable.template_to_where_clause(template)
            q2 = "DELETE FROM " + self._table_name + " " + where_clause[0]
            print("delete_by_template() method's SQL query: ", q2, "\n")

            result = self._run_q(q2, args=where_clause[1], fields=None, fetch=False, commit=True)

            # showing no. of rows affected: The execute command in the pymsql client returns this value.
            # If you look at the run_q() function I provided, it returns two values. The first one is the number of rows effected.
            #https://github.com/PyMySQL/PyMySQL/blob/f8c31d40c5abda9e03de5df34ea692b428fb6677/pymysql/cursors.py#L144

        except Exception as e:
            logging.error("RDBDataTable's delete_by_template() method exception", exc_info=True)
            raise e

        return result
        #pass


    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
#3
    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """

        sql, args = RDBDataTable.create_update(self._table_name, new_values, template)
        print(sql, "\n", args)

        result = self._run_q(sql, args=args, fetch=False, commit=True)
        print("Result for update_by_template() = ", json.dumps(result, indent=2))
        return result

#4
    def insert(self, new_record, values_tuple):
        """
        * note: this method doesn't have a WHERE clause
        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        #teststring = ','.join(str(x) for x in new_record.values())
        #values_tuple = "(" + teststring + ")"
        #print("Records to insert (a list of tuples): ", vaues_tuple)
        # currently, not in right format - dict_values(['willido02', '1935', '9', '2', 'USA', 'CA', 'Los Angeles

        res = RDBDataTable.create_insert(self._table_name, new_record, values_tuple)
        print(res)
        #print("insert_sql string:", sql, "values to insert: (tuple) ", recordTuple) # fix recordTuple format

        # sample: INSERT INTO classicmodels.offices
        #(city, officeCode, country, phone, territory, addressLine1, postalCode)
        #values('Boston', 9, 'USA', '+1 617 555 1212', 'NA', '1 Government Plaza', '02101')

        preop_rowcount = self.get_row_count()
        print("Pre-operation table row count: ", preop_rowcount, "\n")
        print("res[0]: ", res[0], "\n")
        print("res[1]: ", res[1], "\n")
        result = self._run_q(res[0], args=values_tuple, fields=None, fetch=False, commit=True) # args' tuple of values should be all strings/text

        postop_rowcount = self.get_row_count()
        print("Pre-operation table row count: ", preop_rowcount, "\n"
                     "Difference in row count pre & post-insert record = ", postop_rowcount-preop_rowcount)
        return result


    def get_rows(self):
        return self._rows


#-----------***************************************************8--------------------#

# preliminary testing methods

connect_info = {
        'host': 'localhost',
        'user': 'dbuser',
        'password': 'dbuserdbuser',
        'db': 'lahman2019raw',
        'charset':'utf8mb4',
        'port': 3306
        }

rdb_tbl1 = RDBDataTable("people", connect_info = connect_info, key_columns= None) #table_name, connect_info, key_columns


#template1 = {"birthState": "CA", "nameLast": "Williams"}
#clause = RDBDataTable.template_to_where_clause(template1) # need to put Class name in front of method otherwise error of "met
#print(clause)


### testing RDB: find_by_template() method
#test example 1:
#result_find_by_tmp = rdb_tbl1.find_by_template(template=template1, field_list= ['playerID', "birthState", "nameLast"])
#print("find_by_template returned rows:", result_find_by_tmp, "\n", "No. of returned records:", len(result_find_by_tmp[1])
#test example 2:
#result_find_by_tmp = rdb_tbl1.find_by_template(template=template1, field_list= None)
#print("find_by_template returned rows:", result_find_by_tmp, "\n", "No. of returned records:", len(result_find_by_tmp[1]))


### testing RDB: update_by_template() method
#template2 = {'playerID': 'willibe01'}
#new_values2 = {"deathCountry":"USofA"}
#result_update_bytmp = rdb_tbl1.update_by_template(template=template2, new_values=new_values2)
#print(result_update_bytmp)

# check updated row result:
# template3 = {'playerID': 'willibe01'}
# check_update_by_tmp2 = rdb_tbl1.find_by_template(template=template3, field_list= None)
# print(check_update_by_tmp2)
#
# curr_rowcount1 = rdb_tbl1.get_row_count()
# print("Current table row count:", curr_rowcount1)
# #19617


## testing RDB: delete_by_template()
#template4= {'playerID': 'willido02'}
#result_delete_by_tmp = rdb_tbl1.delete_by_template(template=template4)

#print("delete_by_template returned rows:", result_delete_by_tmp, "\n")
# returned rows: (0, None)
#curr_rowcount2 = rdb_tbl1.get_row_count()
#print("Current table row count:", curr_rowcount2)
# Current table row count: 19616



## testing RDB: insert()

## test case: zip lists of column names & new record values into a dictionary
cols = ['playerID','birthYear','birthMonth','birthDay','birthCountry','birthState','birthCity','deathYear','deathMonth','deathDay','deathCountry','deathState','deathCity','nameFirst','nameLast','nameGiven','weight','height','bats','throws','debut','finalGame','retroID','bbrefID']
values = ['williwonka', '2019', '9', '2', 'USA', 'CA', 'Los Angeles', '1991', '12', '20', 'USA', 'CA', 'La Jolla', 'Don', 'Williams', 'Donald Reid', '218', '77', 'R', 'R', '1963-08-04', '1963-08-17', 'willd110', 'willido02']
testinsertdict = dict(zip(cols, values))

##teststring = ','.join(str(x) for x in testdict.values())
##values_tuple = "(" + teststring + ")"
##print(values_tuple)

#values_tuple = ('williwonka', '2019', '9', '2', 'USA', 'CA', 'Los Angeles', '1991', '12', '20', 'USA', 'CA', 'La Jolla', 'Don', 'Williams', 'Donald Reid', '218', '77', 'R', 'R', '1963-08-04', '1963-08-17', 'willd110', 'willido02')

#new_record = testinsertdict
#result_insert = rdb_tbl1.insert(new_record, values_tuple)
## successfully inserted above new people record

## error message received when trying to re-insert the same new record due to duplicate entry detected by MySQL:
# pymysql.err.IntegrityError: (1062, "Duplicate entry 'williwonka' for key 'PRIMARY'")


