API Reference
=============

.. py:function:: frame_to_hyper(df: pd.DataFrame, database: Union[str, pathlib.Path], *, table: Union[str, tableauhyperapi.Name, tableauhyperapi.TableName], table_mode: str = "w", not_null_columns: Optional[Iterable[str]] = None, json_columns: Optional[Iterable[str]] = None, geo_columns: Optional[Iterable[str]] = None) -> None:

   Convert a DataFrame to a .hyper extract.

   :param df: Data to be written out.
   :param database: Name / location of the Hyper file to write to.
   :param table: Table to write to.
   :param table_mode: The mode to open the table with. Default is "w" for write, which truncates the file before writing. Another option is "a", which will append data to the file if it already contains information.
   :param not_null_columns: Columns which should be considered "NOT NULL" in the target Hyper database. By default, all columns are considered nullable
   :param json_columns: Columns to be written as a JSON data type
   :param geo_columns: Columns to be written as a GEOGRAPHY data type

.. py:function:: frame_from_hyper(source: Union[str, pathlib.Path, tab_api.Connection], *, table: Union[str, tableauhyperapi.Name, tableauhyperapi.TableName], return_type: Literal["pandas", "pyarrow", "polars"] = "pandas")

   Extracts a DataFrame from a .hyper extract.

   :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
   :param table: Table to read.
   :param return_type: The type of DataFrame to be returned


.. py:function:: frames_to_hyper(dict_of_frames: Dict[Union[str, tableauhyperapi.Name, tableauhyperapi.TableName], pd.DataFrame], database: Union[str, pathlib.Path], *, table_mode: str = "w", not_null_columns: Optional[Iterable[str]] = None, json_columns: Optional[Iterable[str]] = None, geo_columns: Optional[Iterable[str]] = None,) -> None:

   Writes multiple DataFrames to a .hyper extract.

   :param dict_of_frames: A dictionary whose keys are valid table identifiers and values are dataframes
   :param database: Name / location of the Hyper file to write to.
   :param table_mode: The mode to open the table with. Default is "w" for write, which truncates the file before writing. Another option is "a", which will append data to the file if it already contains information.
   :param not_null_columns: Columns which should be considered "NOT NULL" in the target Hyper database. By default, all columns are considered nullable
   :param json_columns: Columns to be written as a JSON data type
   :param geo_columns: Columns to be written as a GEOGRAPHY data type

.. py:function:: frames_from_hyper(source: Union[str, pathlib.Path, tab_api.Connection], *, return_type: Literal["pandas", "pyarrow", "polars"] = "pandas") -> dict:

   Extracts tables from a .hyper extract.

   :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
   :param return_type: The type of DataFrame to be returned


.. py:function:: frame_from_hyper_query(source: Union[str, pathlib.Path, tab_api.Connection], query: str, *, return_type: Literal["pandas", "polars", "pyarrow"] = "pandas",)

   Executes a SQL query and returns the result as a pandas dataframe

   :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
   :param query: SQL query to execute.
   :param return_type: The type of DataFrame to be returned
