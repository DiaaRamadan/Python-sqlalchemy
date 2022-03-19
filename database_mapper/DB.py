from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine


class DB:
    __engin = None
    __where = []

    def __init__(self, username, password, host, database, port=5432):
        self.__username = username
        self.__password = password
        self.__host = host
        self.__port = port
        self.__database = database
        self.__create_engin()

    def query(self, sql: str):
        if self.__engin is None:
            raise Exception("Can't connect to database")
        return self.__engin.execute(sql)

    def where(self, where: list):
        if not self.__is_valid_where(where):
            raise Exception("Bad argument")
        where.append('and')
        self.__where.append(where)
        return self

    def or_where(self, where: list):
        if not self.__is_valid_where(where):
            raise Exception("Bad arguments")
        where.append('or')
        self.__where.append(where)
        return self

    def delete(self, table: str):
        where = self.__prepare_where_for_query()
        sql = f"DELETE FROM {table} "
        sql += f"where {where}" if where != '' else ''
        return self.query(sql)

    def insert(self, table: str, values: dict):
        """Data is a dictionary field as a key and value is value to insert in db"""
        if len(values) == 0:
            raise Exception("Invalid arguments, Please insert values")
        return self.query(f"INSERT INTO {table}{self.__prepare_insert_value(values)}")

    def get(self, table: str, columns=None):
        if columns is None:
            columns = []
        columns_str = '*' if len(columns) == 0 else ','.join(columns)
        where = self.__prepare_where_for_query()
        sql = f"select {columns_str} FROM {table} "
        sql += f"where {where}" if where != '' else ''
        return self.query(sql).fetchall()

    def __prepare_where_for_query(self):
        where = ''
        count = 1
        for item in self.__where:
            operator = item[3] if count > 1 else ''
            where += operator + ' ' + str(item[0]) + str(item[1]) + str(item[2]) + ' '
            count += 1
        return where

    @staticmethod
    def __prepare_insert_value(values: dict):
        fields = ''
        data_to_insert = ''
        count = 1
        for field, value in values.items():
            fields += str(field)
            data_to_insert += str(value)
            if count != len(values):
                fields += ','
                data_to_insert += ','
            count += 1
        return f"({fields}) VALUES({data_to_insert})"

    @staticmethod
    def __is_valid_where(where: list) -> bool:
        return len(where) == 3

    def __prepare_connection_url(self):
        return URL.create(drivername='postgresql', username=self.__username,
                          password=self.__password, host=self.__host, port=self.__port,
                          database=self.__database)

    def __create_engin(self):
        url = self.__prepare_connection_url()
        if url is None:
            raise Exception("Invalid arguments")
        self.__engin = create_engine(url=url)
