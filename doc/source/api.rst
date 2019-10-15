API Reference
=============

.. py:function:: frame_to_hyper(df: pd.DataFrame, database: str, *, table: str, schema[Optional[str]] = None) -> None:

   Convert a DataFrame to a .hyper extract.

   :param pd.DataFrame df: Data to be written out.
   :param str database: Name / location of the Hyper file to write to.
   :param str table: Table to write to. Must be supplied as a keyword argument.
   :param schema: Schema to write to. Must be supplied as a keyword argument.
   :type schema: str, optional


.. py:function:: frame_from_hyper(database: str, *, table: str, schema: str) -> pd.DataFrame:

   Extracts a DataFrame from a .hyper extract.

   :param str database: Name / location of the Hyper file to be read.
   :param str table: Table to read. Must be supplied as a keyword argument.
   :param schema: Schema to read from. Must be supplied as a keyword argument.
   :type schema: str, optional
   :rtype: pd.DataFrame
