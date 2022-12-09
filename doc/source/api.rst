API Reference
=============

.. py:function:: frame_to_hyper(df: pd.DataFrame, database: Union[str, pathlib.Path], *, table: Union[str, tableauhyperapi.Name, tableauhyperapi.TableName], table_mode: str = "w", hyper_process: Optional[HyperProcess]) -> None:

   Convert a DataFrame to a .hyper extract.

   :param df: Data to be written out.
   :param database: Name / location of the Hyper file to write to.
   :param table: Table to write to. Must be supplied as a keyword argument.
   :param table_mode: The mode to open the table with. Default is "w" for write, which truncates the file before writing. Another option is "a", which will append data to the file if it already contains information.
   :param hyper_process: A `HyperProcess` in case you want to spawn it by yourself. Optional. Must be supplied as a keyword argument.
   :param use_parquet: Use a temporary parquet file to write into the Hyper database, which typically will yield better performance. Boolean, default False


.. py:function:: frame_from_hyper(source: Union[str, pathlib.Path, tab_api.Connection], *, table: Union[str, tableauhyperapi.Name, tableauhyperapi.TableName], hyper_process: Optional[HyperProcess], use_float_na: bool = False) -> pd.DataFrame:

   Extracts a DataFrame from a .hyper extract.

   :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
   :param table: Table to read. Must be supplied as a keyword argument.
   :param hyper_process: A `HyperProcess` in case you want to spawn it by yourself. Optional. Must be supplied as a keyword argument.
   :param use_float_na: Flag indicating whether to use the pandas `Float32`/`Float64` dtypes which support the new pandas missing value  `pd.NA`, default False
   :rtype: pd.DataFrame


.. py:function:: frames_to_hyper(dict_of_frames: Dict[Union[str, tableauhyperapi.Name, tableauhyperapi.TableName], pd.DataFrame], database: Union[str, pathlib.Path], table_mode: str = "w", *, hyper_process: Optional[HyperProcess]) -> None:

   Writes multiple DataFrames to a .hyper extract.

   :param dict_of_frames: A dictionary whose keys are valid table identifiers and values are dataframes
   :param database: Name / location of the Hyper file to write to.
   :param table_mode: The mode to open the table with. Default is "w" for write, which truncates the file before writing. Another option is "a", which will append data to the file if it already contains information.
   :param hyper_process: A `HyperProcess` in case you want to spawn it by yourself. Optional. Must be supplied as a keyword argument.
   :param use_parquet: Use a temporary parquet file to write into the Hyper database, which typically will yield better performance. Boolean, default False

.. py:function:: frames_from_hyper(source: Union[str, pathlib.Path, tab_api.Connection], *, hyper_process: Optional[HyperProcess]) -> Dict[tableauhyperapi.TableName, pd.DataFrame, use_float_na: bool = False]:

   Extracts tables from a .hyper extract.

   :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
   :param hyper_process: A `HyperProcess` in case you want to spawn it by yourself. Optional. Must be supplied as a keyword argument.
   :param use_float_na: Flag indicating whether to use the pandas `Float32`/`Float64` dtypes which support the new pandas missing value  `pd.NA`, default False
   :rtype: Dict[tableauhyperapi.TableName, pd.DataFrame]


.. py:function:: frame_from_hyper_query(source: Union[str, pathlib.Path, tab_api.Connection], query: str, *, hyper_process: Optional[HyperProcess], use_float_na: bool = False) -> pd.DataFrame:

.. versionadded:: 2.0

   Executes a SQL query and returns the result as a pandas dataframe

   :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
   :param query: SQL query to execute.
   :param hyper_process: A `HyperProcess` in case you want to spawn it by yourself. Optional. Must be supplied as a keyword argument.
   :param use_float_na: Flag indicating whether to use the pandas `Float32`/`Float64` dtypes which support the new pandas missing value  `pd.NA`, default False
   :rtype: Dict[tableauhyperapi.TableName, pd.DataFrame]
