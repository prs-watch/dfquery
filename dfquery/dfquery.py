import sqlite3
import pandas as pd
import os
import uuid
import sqlparse


class DFQueryError(Exception):
    """
    wrapped exception.
    """

    pass


class DFQuery:
    """
    client class.
    """

    def __init__(self, gvars):
        """
        initialize.

        Parameters
        ----------
        gvars : List
            global variables.
        """
        self.vars = gvars
        self.dbid = str(uuid.uuid4())

    # --- private functions ---

    def __get_sql_conn(self):
        """
        create sql connection.

        Returns
        -------
        connection : Connection
            connection to SQLite.
        """
        return sqlite3.connect(self.dbid)

    def __close_sql_conn(self, conn):
        """
        close sql connection.

        Parameters
        ----------
        conn : Connection
            connection that is closed.
        """
        conn.close()

    def __judge_query_type(self, query, crud_type):
        """
        judge whether passed query is correct for called function.

        Parameters
        ----------
        query : str
            SQL query.
        crud_type : str
            select/create/delete/update

        Returns
        -------
        result : bool
            judgement result.
        """
        tokens = list(sqlparse.parse(query)[0].flatten())
        for t in tokens:
            if t.ttype == sqlparse.tokens.Keyword.DML:
                if t.value.upper() == crud_type.upper():
                    return True
        return False

    def __get_var_name(self, var):
        """
        get object's variable name.

        Parameters
        ----------
        var : object
            object.

        Returns
        -------
        name : str
            variable name.
        """
        for k, v in self.vars.items():
            if id(v) == id(var):
                return k

    # --- public functions ---

    def read(self, dataframe, query, resources=[]):
        """
        interface to execute SELECT statement.

        Parameters
        ----------
        dataframe : DataFrame
            dataframe to SELECT.
        query : str
            SELECT query.
        resources : List
            DataFrame list that is used as inner table.

        Returns
        -------
        result : DataFrame
            SELECT result.
        """
        if not self.__judge_query_type(query, "select"):
            raise DFQueryError("Not SELECT query is not supported by this function.")

        conn = self.__get_sql_conn()
        try:
            dataframe.to_sql(
                self.__get_var_name(dataframe),
                con=conn,
                index=False,
                if_exists="replace",
            )
            if len(resources) != 0:
                for r in resources:
                    r.to_sql(
                        self.__get_var_name(r),
                        con=conn,
                        index=False,
                        if_exists="replace",
                    )
            return pd.read_sql_query(query, conn)
        finally:
            self.__close_sql_conn(conn)

    def update(self, dataframe, query, resources=[]):
        """
        interface to execute UPDATE statement.

        Parameters
        ----------
        dataframe : DataFrame
            dataframe to UPDATE.
        query : str
            SELECT query.
        resources : List
            DataFrame list that is used as inner table.

        Returns
        -------
        result : DataFrame
            UPDATE result.
        """
        if not self.__judge_query_type(query, "update"):
            raise DFQueryError("Not UPDATE query is not supported by this function.")

        conn = self.__get_sql_conn()
        target = self.__get_var_name(dataframe)
        try:
            dataframe.to_sql(target, con=conn, index=False, if_exists="replace")
            if len(resources) != 0:
                for r in resources:
                    r.to_sql(
                        self.__get_var_name(r),
                        con=conn,
                        index=False,
                        if_exists="replace",
                    )
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return pd.read_sql_query(f"select * from {target}", con=conn)
        finally:
            self.__close_sql_conn(conn)

    def execute(self, query):
        """
        interface to execute some statement.

        Parameters
        ----------
        query : str
            SELECT query.
        """
        conn = self.__get_sql_conn()
        try:
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
        finally:
            self.__close_sql_conn(conn)

    def close(self):
        """
        remove temp SQLite.
        """
        os.remove(self.dbid)
