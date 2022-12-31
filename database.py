import sqlite3


class Database:
    """
    A class to manage project's database

    Attributes
    ----------
    connection:
        Object to manage connection to project's database
    cursor:
        Cursor of connection to the project's database
    """
    def __init__(self, database_name):
        """
        Constructs all necessary attributes for the database object.

        :param database_name: str,
            name of database to connect with
        """
        self.connection = sqlite3.connect(database_name)
        self.cursor = self.connection.cursor()

    def __del__(self):
        """Closes connection to project's database"""
        self.connection.close()

    def create_table(self, table, columns):
        """
        Creates table with provided name if it does not exist.

        :param table: str,
             name of table to create
        :param columns: list[str]
            column's table for table to create
        :return:
            None
        """
        sql = f'CREATE TABLE IF NOT EXISTS {table}({columns})'
        self.cursor.execute(sql)
        self.connection.commit()

    def insert(self, table, *values):
        """
        Inserts data into provided table.

        :param table: str,
            table's name where data will be inserted.
        :param values: list [Any],
            data which will be inserted in the table.
        :return:
            None.
        """
        self.cursor.execute(f"INSERT INTO {table} VALUES ({','.join(['?' for _ in values])})", values)
        self.connection.commit()

    def get(self, target, table, condition):
        """
        Gets data from the project's database.

        :param target: sql,
            part of sql statement which should be put after 'SELECT' but before 'FROM' in sql query.
        :param table: str,
            name of table from which data will be provided.
        :param condition: sql
            part of sql statement which should be put after 'WHERE' in sql query.
        :return:
            tuple.
        """
        return tuple(self.cursor.execute(f"SELECT {target} FROM {table} WHERE {condition}"))[0]

    def update(self, table, target, condition):
        """
        Updates data for the project's database.

        :param table: str,
            name of table for which data will be updated.
        :param target: sql,
            part of sql statement which should be put after 'SET' but before 'WHERE' in sql query.
        :param condition: sql,
            part of sql statement which should be put after 'WHERE' in sql query.
        :return:
            None.
        """
        self.cursor.execute(f'UPDATE {table} SET {target} WHERE {condition}')
        self.connection.commit()

    def replace(self, table, columns, values):
        """
        Inserts data or update it if provided row is already in database.

        :param table: str,
            name of table for which data will be inserted/updated.
        :param columns: list [str],
            list of one or more column's name which will be inserted/updated.
        :param values: list [Any],
            list of on or more values which will be inserted or updated with.
        :return:
            None
        """
        sql = f'REPLACE INTO {table} ({columns}) VALUES ({values})'
        self.cursor.execute(sql)
        self.connection.commit()

    def create_index(self, index, table, columns):
        """
        Creates index if not exists for provided column and table.
        :param index: str,
            name of index which will be created.
        :param table: str,
            name of table for which index will be created.
        :param columns: list [str],
            list of one or more column's name on which index will be created.
        :return:
            None
        """
        sql = f'CREATE INDEX IF NOT EXISTS {index} ON {table} ({columns})'
        self.cursor.execute(sql)
        self.connection.commit()


