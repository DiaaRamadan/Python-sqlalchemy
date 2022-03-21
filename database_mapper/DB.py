from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from database_mapper.Where import Where


class DB(Where):
    __engin = None
    __transaction = None
    __transaction_engin = None

    def __init__(self, username, password, host, database, port=5432):
        self.__username = username
        self.__password = password
        self.__host = host
        self.__port = port
        self.__database = database
        self.__create_engine()

    def query(self, sql: str):
        if self.__engin is None:
            raise Exception("Can't connect to database")
        return self.__engin.execute(sql)

    def delete(self, table: str):
        where = self._prepare_where_for_query()
        sql = f"DELETE FROM {table} "
        sql += f"where {where}" if where != '' else ''
        return self.query(sql)

    def insert(self, table: str, values: dict):
        """Data is a dictionary field as a key and value is value to insert in db"""
        if len(values) == 0:
            raise Exception("Invalid arguments, Please insert values")
        return self.query(f"INSERT INTO {table}{self.__prepare_insert_sql(values)}")

    def get(self, table: str, columns: list = None):
        """Columns is a list of columns want to fetch"""
        if columns is None:
            columns = []
        columns_str = '*' if len(columns) == 0 else ','.join(columns)
        where = self._prepare_where_for_query()
        sql = f"select {columns_str} FROM {table} "
        sql += f"where {where}" if where != '' else ''
        return self.query(sql).fetchall()

    def begin_transaction(self):
        if self.__engin is None:
            raise Exception("Can't connect to database")
        self.__transaction_engin = self.__engin
        self.__engin = self.__transaction = self.__engin.connect()
        self.__transaction = self.__engin.begin()

    def commit_transaction(self):
        self.__transaction.commit()
        self.__engin.close()
        self.__engin = self.__transaction_engin

    def rollback_transaction(self):
        self.__transaction.rollback()
        self.__engin.close()
        self.__engin = self.__transaction_engin

    def __prepare_insert_sql(self, values: dict):
        fields = ''
        data_to_insert = ''
        count = 1
        for field, value in values.items():
            fields += str(field)
            fields = self.add_seperator_if_not_last(count, len(values), fields)
            data_to_insert += str(value)
            data_to_insert = self.add_seperator_if_not_last(count, len(values), data_to_insert)
            count += 1
        return f"({fields}) VALUES({data_to_insert})"

    def add_seperator_if_not_last(self, count, length, string_to_append: str) -> str:
        if count != length:
            return self.__add_seperator_to_string(string_to_append, ',')
        return string_to_append

    @staticmethod
    def __add_seperator_to_string(string: str, seperator: str) -> str:
        string += seperator
        return string

    def __prepare_connection_url(self):
        return URL.create(drivername='postgresql', username=self.__username,
                          password=self.__password, host=self.__host, port=self.__port,
                          database=self.__database)

    def __create_engine(self):
        url = self.__prepare_connection_url()
        if url is None:
            raise Exception("Invalid arguments")
        self.__engin = create_engine(url=url)
