Usage Limitations
=================

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
|bool                |BOOL             |NOT NULLABLE|
+--------------------+-----------------+------------+
|datetime64[ns]      |TIMESTAMP        |NULLABLE    |
+--------------------+-----------------+------------+
|datetime64[ns, UTC] |TIMESTAMP_WITH_TZ|Nullable    |
+--------------------+-----------------+------------+
|timedelta64[ns]     |INTERVAL         |NULLABLE    |
+--------------------+-----------------+------------+
|object              |TEXT             |NULLABLE    |
+--------------------+-----------------+------------+

Any dtype not explicitly listed in the above table will raise a ValueError if trying to write out data.

.. warning::

   ``object`` dtype is a very generic type in pandas, but is the closest type to a native "text" type available prior to 1.0.0. Passing non-string / NULL data through an ``object`` dtype will raise a TypeError, though it is perfectly valid for storage in pandas. This may change in the future as pandas 1.0.0 introduces a dedicated "String" data type.

.. note::

   All objects can maintain their type when "round-tripping" to/from Hyper extracts, with the exception of the float32 object as only DOUBLE is available for floating point storage in Hyper. 

Missing Value Handling
----------------------
In the scientific Python community missing values are often notated as ``np.nan``, which is a float value implementing the `IEEE 754 <https://en.wikipedia.org/wiki/IEEE_754>`_ standard for missing value handling. Because NumPy stores homogenous arrays, there has been a long standing issue in pandas where integers could not store missing records, as noted in the `pandas documentation on missing values <https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html#working-with-missing-data>`_.

Complicating factors is the presence of the ``None`` sentinel in Python, which can often times intuitively be represented as a missing value. Unfortunately ``None`` is a Python level construct, and most of the performance of the data stack in Python is implemented in lower-level C routines that cannot take advantage of this value. The Hyper API though uses this as its missing value indicator.

Because of the various missing value indicators, serializing to/from a Hyper extract via pantab cannot be lossless. On write, ``None`` and ``np.nan`` values will both be written in as missing data to the Hyper extract. On read, missing values will be read back as ``np.nan``.

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
