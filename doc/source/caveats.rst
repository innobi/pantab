Usage Notes
===========

TableauHyperAPI Compatability
-----------------------------

pantab leverages C/C++ code to reduce any Python overhead and maximize performance of reads / writes. Unfortunately, the Tableau Hyper API does not currently make guarantees around ABI stability, so not every version of pantab works with every version of the Hyper API. The known compatability matrix is:

+-------+-----------+
|pantab |Hyper      |
|version|API        |
|       |version    |
+=======+===========+
|<=2.1.X|<=0.0.14567|
+-------+-----------+
|>=3.X  |>=0.0.14567|
+-------+-----------+

If you are using supported versions and still encounter runtime compatability issues, please open a bug report on the `pantab Github issues page <https://github.com/innobi/pantab/issues>`_


Type Mapping
------------

pantab maps the following dtypes from pandas to the equivalent column type in Tableau.

+--------------------+-----------------+------------+
|Dtype               |Tableau Type     |Nullability |
+====================+=================+============+
|int16               |SMALLINT         |NOT NULLABLE|
+--------------------+-----------------+------------+
|int32               |INT              |NOT NULLABLE|
+--------------------+-----------------+------------+
|int64               |BIGINT           |NOT NULLABLE|
+--------------------+-----------------+------------+
|Int16               |SMALLINT         |NULLABLE    |
+--------------------+-----------------+------------+
|Int32               |INT              |NULLABLE    |
+--------------------+-----------------+------------+
|Int64               |BIGINT           |NULLABLE    |
+--------------------+-----------------+------------+
|float32             |DOUBLE           |NULLABLE    |
+--------------------+-----------------+------------+
|float64             |DOUBLE           |NULLABLE    |
+--------------------+-----------------+------------+
|Float32             |DOUBLE           |NULLABLE    |
+--------------------+-----------------+------------+
|Float64             |DOUBLE           |NULLABLE    |
+--------------------+-----------------+------------+
|bool                |BOOL             |NOT NULLABLE|
+--------------------+-----------------+------------+
|boolean             |BOOL             |NULLABLE    |
+--------------------+-----------------+------------+
|datetime64[ns]      |TIMESTAMP        |NULLABLE    |
+--------------------+-----------------+------------+
|datetime64[ns, UTC] |TIMESTAMP_WITH_TZ|Nullable    |
+--------------------+-----------------+------------+
|timedelta64[ns]     |INTERVAL         |NULLABLE    |
+--------------------+-----------------+------------+
|object              |TEXT             |NULLABLE    |
+--------------------+-----------------+------------+
|string              |TEXT             |NULLABLE    |
+--------------------+-----------------+------------+

Any dtype not explicitly listed in the above table will raise a ValueError if trying to write out data.

When reading data through ``frame_from_hyper_query``, pantab will always assume that all result columns are nullable.

.. versionadded:: 1.0.0
   If using pandas 1.0 and above, text data will be read back into a ``string`` dtype rather than an ``object`` dtype.

.. note::

   Most objects can maintain their type when "round-tripping" to/from Hyper extracts, with the exception of the float32 object as only DOUBLE is available for floating point storage in Hyper. After pandas 1.0 / pantab 1.0, object dtypes written will be read back in as string. Also, reading data back through ``frame_from_hyper_query`` will likely read back different dtypes, as ``frame_from_hyper_query`` assumes all columns to be nullable.

Index / Column Handling
-----------------------
A pandas DataFrame always comes with an ``Index`` used to slice / access rows. No such concept exists in Tableau, so this will be implicitly dropped when writing Hyper extracts.

With respect to columns, note that Tableau stores column labels internally as a string. You *may* be able to write non-string objects to the database (this is left to the underlying Hyper API to decide) but reading those objects back is not a lossless operation and will always return strings.

Datetime Timezone Handling
--------------------------

Timezones are not supported in Hyper Extracts. Attempting to write a timezone aware array to an extract will result in an error that the dtype is not supported. The only option to write dates with timezone information would be to make the data timezone naive. You may also consider storing the timezone in a separate column as part of the extract to avoid losing information.

Timedelta Components
--------------------

The ``pd.Timedelta`` and the ``Interval`` exposed by the Hyper API have similar but different storage mechanisms that may cause inconsistencies. Specifically, a `pd.Timedelta` does not have a month component to it, so reading ``Interval`` objects from a Hyper Extract that have this component will raise a ``TypeError``.  The Hyper API's ``Interval`` only offers storage of days and microseconds (aside from months). pantab will convert hours, minutes, seconds, etc... into microseconds for you, but reading that information back from a Hyper extract is lossy and will only provide back the microsecond storage.
