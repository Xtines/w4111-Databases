# W4111_F19_HW1
Implementation template for homework 1.


#### Highlights of design choices/implementation approach

In the below section I include brief highlights and explanations of how I implemented some of my methods.


### RDBDataTable method highlight

#### MySQL retrieval of a table's primary keys within RDBDataTable class constructor

For the purpose of retrieving and store a list of a table's primary keys which have already been defined in the MySQL database, I included a function that queries MySQL database to look up the primary keys associated with a given table.

In essence, we are asking calling this SQL query "SHOW KEYS FROM table WHERE Key_name = 'PRIMARY'" to fetch those columns that have "PRIMARY" associated with their "Key_name", then storing these returned column names in a list, which is then saved as "self._key_columns".  


``` python
self._key_columns = self._get_primarykeys()

    def _get_primarykeys(self):

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

```

Result output for primary key retrieval: 
```
rows: (3, [{'Table': 'appearances', 'Non_unique': 0, 'Key_name': 'PRIMARY', 'Seq_in_index': 1, 'Column_name': 'yearID', 'Collation': 'A', 'Cardinality': 137, 'Sub_part': None, 'Packed': None, 'Null': '', 'Index_type': 'BTREE', 'Comment': '', 'Index_comment': '', 'Visible': 'YES', 'Expression': None}, {'Table': 'appearances', 'Non_unique': 0, 'Key_name': 'PRIMARY', 'Seq_in_index': 2, 'Column_name': 'teamID', 'Collation': 'A', 'Cardinality': 3276, 'Sub_part': None, 'Packed': None, 'Null': '', 'Index_type': 'BTREE', 'Comment': '', 'Index_comment': '', 'Visible': 'YES', 'Expression': None}, {'Table': 'appearances', 'Non_unique': 0, 'Key_name': 'PRIMARY', 'Seq_in_index': 3, 'Column_name': 'playerID', 'Collation': 'A', 'Cardinality': 100197, 'Sub_part': None, 'Packed': None, 'Null': '', 'Index_type': 'BTREE', 'Comment': '', 'Index_comment': '', 'Visible': 'YES', 'Expression': None}])

Retrieving primary keys from returned 'Column_name' in db: yearID
Retrieving primary keys from returned 'Column_name' in db: teamID
Retrieving primary keys from returned 'Column_name' in db: playerID
Primary key(s) for  appearances include:  ['yearID', 'teamID', 'playerID']

```


### CSVDataTable method highlight

#### Update_by_template() method approach using Pandas dataframe operations 


```
def update_by_template(self, template, new_values_dict):

    self._rows = self._update_row(row_to_update, new_values_dict)

```

My update_by_template() method calls the function `_update_row()` with two provided dictionary inputs: `row_to_update` and `new_values_dict`. 

```
    def _update_row(self, row_to_update, new_values_dict):
        if self._rows is None:
            self._rows = []

        key_cols = self._data["key_columns"]

        ## df1 containing all records in original table
        df1 = pd.DataFrame(self._rows, columns = self._data["table_columns"]) # df1 = pd.DataFrame(data1, columns= ['Code','Name','Value'])
        
        ## df2 for dict containing associated cols & new values, using new_values_dict
        df2 = pd.DataFrame(new_values_dict, columns=list(new_values_dict.keys()), index=[0])

        df1.set_index(key_cols, inplace=True)
        df1.update(df2.set_index(key_cols))
        df1.reset_index(inplace=True)

        self._rows = df1.to_dict(into=OrderedDict, orient='records')
        return self._rows

```

The main steps involved in `_update_row()` include:

* converting the original table rows/records into a Pandas dataframe
* converting new_values_dict (the dictionary containing fields & their new values to update) into a Pandas dataframe
* using the Pandas library's `.update()` function, which modifies a dataframe in place using non-NA values from another DataFrame.

For example, in the function here, the `.update()` function updates those records in `df1` whose index/primary key(s) matches those in `df2`, which I presume is a row containing the neccessary primary key(s) and new values we wish to update the original record with. 
```
    df1.update(df2.set_index(key_cols))
```
* lastly, converting the updated table results in `df1` back into an OrderedDict format to store/write it back to `self._rows`. 


