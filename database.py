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

    def get(self, table, target, condition_key, condition_value):
        return self.cursor.execute(f"SELECT {target} FROM {table} WHERE {condition_key} = '{condition_value}'")

    def get2(self, target, table, condition):
        return self.cursor.execute(f"SELECT {target} FROM {table} WHERE {condition}")

    def update(self, table, target, target_value, condition_key, condition_value):
        self.cursor.execute(f"UPDATE {table} SET {target} = {target_value} WHERE {condition_key} = '{condition_value}'")
        self.connection.commit()


