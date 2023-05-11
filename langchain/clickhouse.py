from __future__ import annotations

import warnings
from typing import Any, Iterable, List, Optional
import clickhouse_connect

class ClickHouseDataBase:
    """ClickHouse wrapper for database operations."""

    def __init__(
        self,
        database: str,
        host: str,
        port: int = 9000,
        user: Optional[str] = None,
        password: Optional[str] = None,
        include_tables: Optional[List[str]] = None,
        ignore_tables: Optional[List[str]] = None,
        custom_table_info: Optional[dict] = None,
        sample_rows_in_table_info: int = 0,
        indexes_in_table_info: bool = False,
    ):
        """
        Create a ClickHouse database connection.

        Args:
            database: The name of the database to connect to.
            host: The host name or IP address of the ClickHouse server.
            port: The port number to use for the ClickHouse server.
            user: The user name to use for authentication.
            password: The password to use for authentication.
            include_tables: A list of table names to include in the usable tables.
            ignore_tables: A list of table names to ignore in the usable tables.
            custom_table_info: A dictionary mapping table names to custom table info. This will overwrite the default table info. Example here: https://python.langchain.com/en/latest/modules/chains/examples/sqlite.html#custom-table-info 
            sample_rows_in_table_info: The number of sample rows to include in the table info. The table must have sampling enabled or this will throw an error.
            indexes_in_table_info: Whether to include indexes in the table info.
        """
        self._conn = clickhouse_connect.get_client(
            host=host, port=port, user=user, password=password, database=database,
        )

        self._database = database
        self._indexes_in_table_info = indexes_in_table_info
        self._sample_rows_in_table_info = sample_rows_in_table_info
        self._include_tables = set(include_tables) if include_tables else set()
        self._ignore_tables = set(ignore_tables) if ignore_tables else set()
        self._all_tables = set(self.get_all_table_names())
        
        usable_tables = self.get_usable_table_names()
        self._usable_tables = set(usable_tables)

        if self._include_tables and self._ignore_tables:
            raise ValueError("Cannot specify both include_tables and ignore_tables")

        self._custom_table_info = custom_table_info or {}
        if self._custom_table_info:
            if not isinstance(self._custom_table_info, dict):
                raise TypeError(
                    "table_info must be a dictionary with table names as keys and the "
                    "desired table info as values"
                )
            # only keep the tables that are also present in the database
            intersection = set(self._custom_table_info).intersection(self._usable_tables)
            self._custom_table_info = dict(
                (table, self._custom_table_info[table])
                for table in self._custom_table_info
                if table in intersection
            )

    def get_usable_table_names(self) -> Iterable[str]:
        """Get names of tables available."""  
        if self._include_tables:
            missing_tables = self._include_tables - self._all_tables
            if missing_tables:
                raise ValueError(
                    f"include_tables {missing_tables} not found in database"
                )
            usable_tables = self._include_tables
        else:
            usable_tables = self._all_tables - self._ignore_tables      
        return usable_tables

    def get_table_names(self) -> Iterable[str]:
        """Get names of tables available."""
        warnings.warn(
            "This method is deprecated - please use `get_usable_table_names`."
        )
        return self.get_usable_table_names()
    
    def get_all_table_names(self) -> List[str]:
        query = "SHOW TABLES"
        result = self._conn.query(query)
        table_names = [row[0] for row in result.result_rows]
        return table_names

    def get_table_info(self, table_names: Optional[List[str]] = None) -> str:
        """Get information about specified tables.

        Follows best practices as specified in: Rajkumar et al, 2022
        (https://arxiv.org/abs/2204.00498)

        If `sample_rows_in_table_info`, the specified number of sample rows will be
        appended to each table description. This can increase performance as
        demonstrated in the paper.
        """
        all_table_names = self.get_usable_table_names()
        if table_names is not None:
            missing_tables = set(table_names).difference(all_table_names)
            if missing_tables:
                raise ValueError(f"table_names {missing_tables} not found in database")
            all_table_names = table_names


        tables = []
        for table_name in all_table_names:
            table_query = f"DESCRIBE {table_name}"
            table_result = self._conn.query(table_query)
            table_column_names = [row[0] for row in table_result.result_rows]
            table_info = f"{str(table_column_names)}"
            
            if self._custom_table_info and table_name in self._custom_table_info:
                tables.append(str(self._custom_table_info[table_name]))
                continue

            has_extra_info = (
                self._indexes_in_table_info or self._sample_rows_in_table_info
            )
            if has_extra_info:
                table_info += "\n\n/*"
            if self._indexes_in_table_info:
                indexes_query = f"SELECT name FROM system.columns WHERE database = '{self._database}' AND table = '{table_name}' AND is_in_primary_key=1"
                indexes_result = self._conn.query(indexes_query)
                indexes = ""
                for column in indexes_result.result_columns:
                    indexes += f"Primary keys are {column}\n"
                table_info += f"\n{indexes}\n"

            if self._sample_rows_in_table_info:
                try: 
                    sample_query = f"SELECT * FROM {table_name} SAMPLE 0.3 LIMIT {self._sample_rows_in_table_info}"
                    sample_result = self._conn.query(sample_query)
                    columns = [column for column in sample_result.column_names]
                    rows = [tuple(row) for row in sample_result.result_rows]
                    sample_rows = "\n".join([str(row) for row in rows])
                    table_info += f"\n\n-- Sample Rows:\n-- {', '.join(columns)}\n{sample_rows}\n"
                except Exception:
                    print(f"Table {table_name} cannot be sampled. Check the database connection parameters and remove the `sample_rows_in_table_info` parameter or set it to 0.")
            if has_extra_info:
                table_info += "*/"
            tables.append(table_info)

        final_str = "\n\n".join(tables)
        return final_str

    def get_table_info_no_throw(self, table_names: Optional[List[str]] = None) -> str:
        """Get information about specified tables.

        Follows best practices as specified in: Rajkumar et al, 2022
        (https://arxiv.org/abs/2204.00498)

        If `sample_rows_in_table_info`, the specified number of sample rows will be
        appended to each table description. This can increase performance as
        demonstrated in the paper.
        """
        try:
            return self.get_table_info(table_names)
        except ValueError as e:
            """Format the error message"""
            return f"Error: {e}"
    
    def run_no_throw(self, command: str) -> str:
        """Execute a clickhouse command and return a string representing the results.

        If the statement returns rows, a string of the results is returned.
        If the statement returns no rows, an empty string is returned.

        If the statement throws an error, the error message is returned.
        """
        try:
            command_result = self._conn.command(command)
            if command_result:
                return str(command_result)
            else:
                return ""
        except Exception as e:
            """Format the error message"""
            return f"Error: {e}"

    @classmethod
    def get_client(
        cls, host: str, port: int, user: str, password: str, database: str, **kwargs: Any
    ) -> ClickHouseDataBase:
        """Create a clickhouse connection from the parameters."""
        client = clickhouse_connect.get_client(host, user, port, password, database, **kwargs)
        return client
    
    @property
    def dialect(self) -> str:
        """Return string representation of dialect to use."""
        return "clickhouse"
