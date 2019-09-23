
from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd
from collections import OrderedDict

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """
        # for later coursework, don't worry about load= and rows= for now.
        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug,
            "table_columns": None # OH_18_sep_2019: 36m
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()


    def get_key_columns(self):
        #print("Type of _data[key_columns]:", type(self._data["key_columns"])) ## a list; <class 'NoneType'> because key_cols not declared when object instantiated
        return self._data["key_columns"] #TypeError: 'NoneType' object is not iterable

    def get_table_columns(self):
        return self._data["table_columns"]


    def __str__(self):
    #no need to work on this function, just use it
        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)


    def _load(self):
        # code that would load the csv file shown in Lecture 2 - 0h:36m
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        delimiter = self._data["connect_info"].get("delimiter", ',')
        full_name = os.path.join(dir_info, file_n)

        t_columns = None

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file, delimiter=delimiter)
            for r in csv_d_rdr: 
                if t_columns is None:
                    t_cols = list(r.keys())
                    self._data["table_columns"] = t_cols  # inserted _data["table_columns"]

                    key_cols = self._data.get('key_columns', None)
                    if key_cols is not None:
                        key_cols = set(key_cols)
                        if not key_cols.issubset(set(t_cols)):
                            raise Exception("Key column not in table columns.")
                    self._data["table_columns"] = t_cols
                        
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")
        print("Full list of table columns:", self._data["table_columns"])


    def save(self, table_name):
        """
        Write the information back to a file.
        :return: None
        """
        #file_dir = "/Users/christinelee/Xtines/COMSw4111/HW_Assignments/HW1_Template/Data/Baseball/"
        fn = file_dir + table_name + "_postchange.csv"
        #self._rows : 100k times OrderedDict of a list of tuples, ex: OrderedDict([('playerID', 'coxda01'), ('yearID', '1983'), ('stint', '1'), ('teamID', 'SLN'), ('lgID', 'NL'), ('G', '12')

        # to be resumed with more time

    # Lecture 2 coverage: 0h40m start -
    # references Lec 1 notebook

    # Lecture 2 9/13: 1h:06
    def key_to_template(self, key):

        tmp ={}
        #print(self._data['key_columns'], "type:", type(self._data['key_columns'])) #['playerID'] type: <class 'list'>
        #for k in self._data['key_columns']:
        #    tmp = {k: key[k]}
        tmp = dict(zip(self._data["key_columns"], key)) # should be self._data["key_columns"]
        print(dict(zip(self._data["key_columns"], key)), "type:", type(dict(zip(self._data["key_columns"], key))))
        return tmp

    # matches_template func: if given a row and a "template / WHERE clause", evaluate if it matches
    @staticmethod
    def matches_template(row, template):
        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break
        return result

        # Given key_fields & your interested field_list,
        #we already know the key_columns of the table,
        #create a dict by zipping together the key_cols & key_fields, then form a tmp


    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The values for the key_columns, in order, to use to find a record. For example,
            for Appearances this could be ['willite01', 'BOS', '1960']
        :param field_list: A subset of the fields of the record to return. The table may have many
            additional columns, but the caller only requests this subset.
        :return: None, or a dictionary containing the requested columns/values for the row.
        """
        key_cols = self._data["key_columns"] #list(self._data["key_columns"].values())
        tmp = dict(zip(key_cols, key_fields))
        result = self.find_by_template(template=tmp, field_list=field_list)
        if result is not None and len(result) > 0:
            result = result[0]
        else:
            result = None
        return result


    # Wizard implementation method for find_by_primary_key():
    # index = self._indexes['PRIMARY']
    # key_cols = index.get_key_columns()

    def project(row, field_list):

        result = {}

        if field_list is None:
            return row
        for f in field_list:
            result[f] = row[f]
        return result


    def _validate_template_and_fields(self, tmp, fields):
        c_set = set(self._data['table_columns']) # column headers when reading in file/instantiating object
        if tmp is not None:
            t_set = set(tmp.keys())
        else:
            t_set = None

        if fields is not None:
            f_set = set(fields)
        else:
            f_set = None
        #Donald F. Ferguson: You do not need to through custom exceptions. You can just print error messages and then raise an exception.
        if f_set is not None and not f_set.issubset(c_set):
            raise Exception("Fields are invalid.") #DataTableException(DataTableException.invalid_input, "Fields are invalid.")
        if t_set is not None and not t_set.issubset(c_set):
            raise Exception("Fields are invalid.") #DataTableException(DataTableException.invalid_input, "Fields are invalid.")
        return True


    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """
        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
            If field_list == None returns matched rows and all columns
        """
        #pass
        self._validate_template_and_fields(template, field_list)

        result = []
        for r in self._rows:
            if self.matches_template(r, template):
                result.append(r)
            elif template == {}: # for the case if template is not specified or is an empty dict: return all rows
                result.append(r)
        return result


    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        pass

    # watch Lecture 2: 0h: 57 min
    def _delete_row(self, r):
        if self._rows is None:
            self._rows = []
        print(type(self._rows))
        self._rows.remove(r)


    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        row_to_delete = self.find_by_template(template=template)
        if row_to_delete is not None and len(row_to_delete) > 0:
            row_to_delete = row_to_delete[0]
        else:
            row_to_delete = None

        result = self._delete_row(row_to_delete)
        return result
        #pass

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

    def _update_row(self, row_to_update, new_values_dict):
        if self._rows is None:
            self._rows = []

        # print("Type for self._rows: ", type(self._rows[1])) #<class 'collections.OrderedDict'>
        key_cols = self._data["key_columns"]

        ## df1 containing all records in original table
        df1 = pd.DataFrame(self._rows, columns = self._data["table_columns"]) # df1 = pd.DataFrame(data1, columns= ['Code','Name','Value'])
        ## df2 for dict containing associated cols & new values, using new_values_dict
        df2 = pd.DataFrame(new_values_dict, columns=list(new_values_dict.keys()), index=[0])

        ## identifying primarykey-values from row_to_update: may not need
        #rows_pkeysvals = record_to_update.get(key_cols[0], "Key not returned")

        ## https://stackoverflow.com/questions/49928463/python-pandas-update-a-dataframe-value-from-another-dataframe
        #df1.set_index('Code', inplace=True) # df.set_index(['year', 'month'])
        #df1.update(df2.set_index('Code'))
        #df1.reset_index()
        df1.set_index(key_cols, inplace=True)
        df1.update(df2.set_index(key_cols))
        df1.reset_index(inplace=True)
        #print("Updated table containing updated row: ", "\n", df1, "\n")

        self._rows = df1.to_dict(into=OrderedDict, orient='records')
        #print("Updated table row results:", json.dumps(self._rows, indent=2))
        return self._rows


    def update_by_template(self, template, new_values_dict):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields. # As it states in the code description, it will be a dictionary of key value pairs for the matched template.
        :return: Number of rows updated.
        """
        # find_by_template() returns a list containing dictionaries. A dictionary is in the list representing each record
        #             that matches the template.
        row_to_update = self.find_by_template(template=template) # list of dict records to be updated

        #if row_to_update is not None and len(row_to_update) > 0:
        #    row_to_update = row_to_update[0] # row_to_update[0]
        #else:
        #    row_to_update = None

        self._rows = self._update_row(row_to_update, new_values_dict) # :return: Number of rows updated.
        #pass

    # need to perform a check whether the new record added is a duplicate row; include in README
    #def _getkey(self):


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        ## Error cases to catch:
        # need to perform a check whether the new record added is a duplicate row
        #key = self._getkey()

        # https://piazza.com/class/jy3jm0i73f8584?cid=230
        # Point: MySQL throws an error if an INSERT query has an alien column. CSVDataTable should replicate the same behavior.

        pass
        #pass


    def get_rows(self):
        return self._rows



def load_csv_file(fn):
    result = []
    with open(fn, "r") as in_file:
        csv_file = csv.DictReader(in_file)
        for r in csv_file:
            result.append(r)
    return result




### Preliminary tests for CSVDataTable methods

file_dir = "/Users/christinelee/Xtines/COMSw4111/HW_Assignments/HW1_Template/Data/Baseball/"
fn = file_dir + "People-smallcopy" + ".csv"
rows = load_csv_file(fn)

print("No of rows = ", len(rows))
print("First 10 rows = \n", json.dumps(rows[0:9], indent=2))

#m1 = CSVDataTable.matches_template(rows, {"birthDay": "25", "birthCountry": "USA"}) # adding self object before function?
#print("Match = ", m1)
#m1 = CSVDataTable.matches_template(rows, {"birthDay": "27","birthCountry": "USA"})
#print("Match = ", m1)

data_dir = os.path.abspath("../Data/Baseball")
connect_info = {
    "directory": data_dir,
    "file_name": "People-smallcopy.csv"
}

# csvDataTable = CSVDataTable('People', connect_info, ['playerID'])
# csvDataTable.find_by_primary_key(['willite01'])

# key columns for batting.csv
#key_cols = ['playerID', 'teamID', 'yearID', 'stint']

csv_tbl3 = CSVDataTable("People-smallcopy", connect_info, key_columns= ['playerID'])

# test getting primary keys
print("get ._data values from key_columns:", csv_tbl3.get_key_columns())
print(type(csv_tbl3._rows[1])) # each is a <class 'list'> of OrderedDicts <class 'collections.OrderedDict'>

#print("length of csv_tbl2._rows:", len(csv_tbl2._rows)) #    df1 = pd.DataFrame([row.split(",") for row in csv_tbl2._rows])
                                                            #AttributeError: 'collections.OrderedDict' object has no attribute 'split'

#import  pandas as pd
#df1 = pd.DataFrame([row.split(",") for row in csv_tbl2._rows])
#print(df1.head())

#key_cols = list(self._data["key_columns"].values())
#print(key_cols)


