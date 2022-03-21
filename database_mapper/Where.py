class Where:
    _where = []

    def where(self, where: list):
        if not self._is_valid_where(where):
            raise Exception("Bad argument")
        where.append('and')
        self._where.append(where)
        return self

    def or_where(self, where: list):
        if not self._is_valid_where(where):
            raise Exception("Bad arguments")
        where.append('or')
        self._where.append(where)
        return self

    def where_raw(self):
        """Write the all whereas sql query"""
        # TODO Where Raw

    @staticmethod
    def _is_valid_where(where: list) -> bool:
        return len(where) == 3

    def _prepare_where_for_query(self):
        where = ''
        count = 1
        for item in self._where:
            operator = item[3] if count > 1 else ''
            where += operator + ' ' + str(item[0]) + str(item[1]) + str(item[2]) + ' '
            count += 1
        return where
