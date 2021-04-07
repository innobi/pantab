API Reference
=============

.. py:function:: frame_to_hyper(df: pd.DataFrame, database: Union[str, pathlib.Path], *, table: Union[str, tableauhyperapi.Name, tableauhyperapi.TableName], hyper_process: Optional[HyperProcess]) -> None:

   Convert a DataFrame to a .hyper extract.

   :param df: Data to be written out.
   :param database: Name / location of the Hyper file to write to.
   :param table: Table to write to. Must be supplied as a keyword argument.
   :param hyper_process: A `HyperProcess` in case you want to spawn it by yourself. Optional. Must be supplied as a keyword argument.


.. py:function:: frame_from_hyper(source: Union[str, pathlib.Path, tab_api.Connection], *, table: Union[str, tableauhyperapi.Name, tableauhyperapi.TableName], hyper_process: Optional[HyperProcess]) -> pd.DataFrame:

   Extracts a DataFrame from a .hyper extract.

   :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
   :param table: Table to read. Must be supplied as a keyword argument.
   :param hyper_process: A `HyperProcess` in case you want to spawn it by yourself. Optional. Must be supplied as a keyword argument.
   :rtype: pd.DataFrame


.. py:function:: frames_to_hyper(dict_of_frames: Dict[Union[str, tableauhyperapi.Name, tableauhyperapi.TableName], pd.DataFrame], database: Union[str, pathlib.Path], *, hyper_process: Optional[HyperProcess]) -> None:

   Writes multiple DataFrames to a .hyper extract.

   :param dict_of_frames: A dictionary whose keys are valid table identifiers and values are dataframes
   :param database: Name / location of the Hyper file to write to.
   :param hyper_process: A `HyperProcess` in case you want to spawn it by yourself. Optional. Must be supplied as a keyword argument.


.. py:function:: frames_from_hyper(source: Union[str, pathlib.Path, tab_api.Connection], *, hyper_process: Optional[HyperProcess]) -> Dict[tableauhyperapi.TableName, pd.DataFrame]:

   Extracts tables from a .hyper extract.

   :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
   :param hyper_process: A `HyperProcess` in case you want to spawn it by yourself. Optional. Must be supplied as a keyword argument.
   :rtype: Dict[tableauhyperapi.TableName, pd.DataFrame]


.. py:function:: frame_from_hyper_query(source: Union[str, pathlib.Path, tab_api.Connection], query: str, *, hyper_process: Optional[HyperProcess]) -> pd.DataFrame:

.. versionadded:: 2.0

   Executes a SQL query and returns the result as a pandas dataframe

   :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
   :param query: SQL query to execute.
   :param hyper_process: A `HyperProcess` in case you want to spawn it by yourself. Optional. Must be supplied as a keyword argument.
   :rtype: Dict[tableauhyperapi.TableName, pd.DataFrame]
