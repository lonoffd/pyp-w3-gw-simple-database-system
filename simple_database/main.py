# -*- coding: utf-8 -*-
import os
from datetime import date
import json

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH


class Row(object):
    def __init__(self, row):
        for key, value in row.items():
            setattr(self, key, value)


class Table(object):

    def __init__(self, db, name, columns=None):
        self.db = db
        self.name = name

        self.table_filepath = os.path.join(BASE_DB_FILE_PATH, self.db.name,
                                           '{}.json'.format(self.name))

        self.columns = columns or self._read_columns()    
        
        if not os.path.isfile(self.table_filepath):
            with open(self.table_filepath, 'w') as file_object:
                json.dump({'columns': columns, 'rows': []}, file_object)
                
    
    def open_json(self):
        with open(self.table_filepath, 'r') as file_object:
            return json.load(file_object)
            
    
    def write_json(self, table_data):
        with open(self.table_filepath, 'w') as file_object:
            json.dump(table_data, file_object)
            
            
    def _read_columns(self):
        # Read the columns configuration from the table's JSON file
        # and return it.
        json_data = self.open_json()
        return json_data['columns']
      

    def insert(self, *args):
        # Validate that the provided row data is correct according to the
        # columns configuration.
        # If there's any error, raise ValidationError exception.
        # Otherwise, serialize the row as a string, and write to to the
        # table's JSON file.

        if len(args) != len(self.columns):
            raise ValidationError("Invalid amount of field")

        the_row_dict = {}
        for arg, column in zip(args, self.columns):
            if not isinstance(arg, eval(column['type'])):
                raise ValidationError('Invalid type of field "{}": Given "{}", expected "{}"'.format(column['name'], type(arg).__name__, column['type']))
            else:
                if isinstance(arg, date):
                    the_row_dict[column['name']] = arg.isoformat()
                else:
                    the_row_dict[column['name']] = arg

        table_data = self.open_json()
        
        table_data['rows'].append(the_row_dict)
        
        self.write_json(table_data)

    def query(self, **kwargs):
        # Read from the table's JSON file all the rows in the current table
        # and return only the ones that match with provided arguments.
        # We would recommend to  use the `yield` statement, so the resulting
        # iterable object is a generator.

        # IMPORTANT: Each of the rows returned in each loop of the generator
        # must be an instance of the `Row` class, which contains all columns
        # as attributes of the object.
        for row in self.all():
            row_is_good = True
            for key, value in kwargs.items():
                if getattr(row, key) != value:
                    row_is_good = False
            if row_is_good:
                yield row
            # check if row is a match with **kwargs
            # if it is, yield it
            # kwargs = {'author': "John", 'nationality': "USA"}

    def all(self):
        # Similar to the `query` method, but simply returning all rows in
        # the table.
        # Again, each element must be an instance of the `Row` class, with
        # the proper dynamic attributes.
        
        table_data = self.open_json()
        for row in table_data['rows']:
            yield Row(row)

    def count(self):
        # Read the JSON file and return the counter of rows in the table
        table_data = self.open_json()
        return len(table_data['rows'])

    def describe(self):
        # Read the columns configuration from the JSON file, and return it.
        return self._read_columns()


class DataBase(object):
    def __init__(self, name):
        self.name = name
        self.db_filepath = os.path.join(BASE_DB_FILE_PATH, self.name)
        self.tables = self._read_tables()

    @classmethod
    def create(cls, name):
        db_filepath = os.path.join(BASE_DB_FILE_PATH, name)
        # if the db directory already exists, raise ValidationError
        # otherwise, create the proper db directory
        if os.path.exists(db_filepath):
            raise ValidationError('Database with name "{}" already exists.'.format(name))
        else:
            os.makedirs(db_filepath)
            

    def _read_tables(self):
        # Gather the list of tables in the db directory looking for all files
        # with .json extension.
        # For each of them, instatiate an object of the class `Table` and
        # dynamically assign it to the current `DataBase` object.
        # Finally return the list of table names.
        # Hint: You can use `os.listdir(self.db_filepath)` to loop through
        #       all files in the db directory
        file_names = os.listdir(self.db_filepath)
        table_names = []
        for file_name in file_names:
            if '.json' in file_name:
                table_name = file_name[:-5]
                new_table = Table(self, table_name)
                setattr(self, table_name, new_table)
                table_names.append(table_name)
        return table_names

    def create_table(self, table_name, columns):
        # Check if a table already exists with given name. If so, raise
        # ValidationError exception.
        # Otherwise, crete an instance of the `Table` class and assign
        # it to the current db object.
        # Make sure to also append it to `self.tables`
        if hasattr(self, table_name):
            raise ValidationError("table already exists")
        elif not isinstance(columns, list):
            raise ValidationError
        else:
            new_table = Table(self, table_name, columns)
            setattr(self, table_name, new_table)
            self._read_tables()

    def show_tables(self):
        # Return the current list of tables.
        return self._read_tables()


def create_database(db_name):
    """
    Creates a new DataBase object and returns the connection object
    to the brand new database.
    """
    DataBase.create(db_name)
    return connect_database(db_name)


def connect_database(db_name):
    """
    Connectes to an existing database, and returns the connection object.
    """
    return DataBase(name=db_name)
