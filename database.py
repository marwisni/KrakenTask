import sqlite3


class Database:
    def __init__(self, database_name):
        self.connection = sqlite3.connect(database_name)
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.connection.close()

    def create_table(self, table, columns):
        sql = f'CREATE TABLE IF NOT EXISTS {table}({columns})'
        self.cursor.execute(sql)
        self.connection.commit()

    def insert(self, table, *values):
        self.cursor.execute(f"INSERT INTO {table} VALUES ({','.join(['?' for _ in values])})", values)
        self.connection.commit()

    def get(self, target, table, condition):
        return tuple(self.cursor.execute(f"SELECT {target} FROM {table} WHERE {condition}"))[0]

    def update(self, table, target, condition):
        self.cursor.execute(f'UPDATE {table} SET {target} WHERE {condition}')
        self.connection.commit()

    def create_index(self, index, table, columns):
        sql = f'CREATE INDEX IF NOT EXISTS {index} ON {table} ({columns})'
        self.cursor.execute(sql)
        self.connection.commit()


