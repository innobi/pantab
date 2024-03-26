=============
Common Issues
=============

pandas type system
------------------

When using pandas as the DataFrame library, the history of the type system in that library can be a common source of confusion. Since pantab 4.0, rather than directly use the pandas type system pantab has used the `Arrow C stream interface <https://arrow.apache.org/docs/format/CStreamInterface.html>`_.

Technically, the way in which pandas types are mapped to Arrow types to fit the Arrow C Stream interface is an implementation detail of pandas that cannot be documented here. However, general guidance for different data types is provided in each section below. When using pandas I/O methods, you may want to use ``dtype_backend="pyarrow"`` to get the best types by default (you can see that keyword argument documented in the `read_csv <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html>`_ documentation.

Object Types
~~~~~~~~~~~~

Generally you should avoid having columns that have ``dtype=object`` in your pandas DataFrame. For more information see :ref:`Strings`

Integral Types
~~~~~~~~~~~~~~

Signed integer types map relatively well to Hyper types regardless of which pandas "backend" you use. Note that the int8 -> SMALLINT case is not lossless, i.e. when you try to read a SMALLINT back from Hyper you will always get a 16 bit integer type back.

+-----+------+--------------+--------+
|numpy|pandas|pyarrow       |Hyper   |
+=====+======+==============+========+
|int8 |Int8  |int8[pyarrow] |SMALLINT|
+-----+------+--------------+--------+
|int16|Int16 |int16[pyarrow]|SMALLINT|
+-----+------+--------------+--------+
|int32|Int32 |int32[pyarrow]|INTEGER |
+-----+------+--------------+--------+
|int64|Int64 |int64[pyarrow]|BIGINT  |
+-----+------+--------------+--------+

Generally unsigned types are not supported - users are expected to bounds check and size to a signed type appropriately. The only exception to this rule is a 32 bit unsigned integer, which is written as an ``OID`` type to Hyper.

Floating-point Types
~~~~~~~~~~~~~~~~~~~~

Hyper only supports double precision floating point types. ``float`` types (often called float32 in the NumPy / pandas world) are upcast to ``double``.

+-------+-------+---------------+---------+
|numpy  |pandas |pyarrow        |Hyper    |
+=======+=======+===============+=========+
|float32|Float32|float[pyarrow] |DOUBLE   |
|       |       |               |PRECISION|
+-------+-------+---------------+---------+
|float64|Float64|double[pyarrow]|DOUBLE   |
|       |       |               |PRECISION|
+-------+-------+---------------+---------+

Strings
~~~~~~~

Much can be written about strings and the history of them in pandas. Generally users use "strings" that are actually any of the following dtypes:

* object
* "string" (starting in 1.0)
* "string[pyarrow]" (starting in 1.3)
* "string[pyarrow_numpy]"  (starting in 3.0)

``object`` is a historic relic and should be avoided where possible. As far as pantab is concerned, using ``string[pyarrow]`` is the best string dtype as that will always map seamlessly to the Arrow C Stream interface. How the other types manage this is an implementation detail of pandas.

Temporal Types
~~~~~~~~~~~~~~

pandas historically has only ever had a ``Timestamp`` data type. This has been problematic when writing to databases where ``DATE`` and ``TIMESTAMP`` are commonly different types.

If you would like to write ``DATE`` types to hyper, your best bet is to ``.astype("date32[pyarrow]")`` those columns to convert them into true date types.  **Do not use the pandas ``.dt.date`` accessor as this returns an ``object`` dtype**.

Please also note that the default unit of precision for the ``pd.Timestamp`` type is nanoseconds since the Unix epoch. Since pandas 2.0 the `as_unit method <https://pandas.pydata.org/docs/dev/reference/api/pandas.Timestamp.as_unit.html#pandas.Timestamp.as_unit>`_ can be used to convert to a different unit. pyarrow supports different units as well, i.e. you can do ``.astype("timestamp[us][pyarrow]")`` to convert to a microsecond-precision timestamp based on the Unix epoch.  Hyper stores timestamps using microsecond precision going back to the first midnight after the Julian epoch.

Binary
~~~~~~

NumPy / pandas do not coordinate to support a binary array type. As such, if you are trying to write binary data, your only option is to use the ``binary[pyarrow]`` data type.
