Usage Limitations
=================

Partial Updates to Extracts
---------------------------
The API currently only supports complete read and writes of a Hyper extract. You can therefore not currently write only to say one table in an extract while preserving all others. Nor can you append rows to existing tables.

As a workaround you can read the entire extract, make the updates you'd like to do in Python and then write the entire extract back. Depending on the size of your dataset this may not scale particularly well, but it is the best option until this functionality gets explicitly implemented.

Missing Value Handling
----------------------
In the scientific Python community missing values are often notated as ``np.nan``, which is a float value implementing the `IEEE 754 <https://en.wikipedia.org/wiki/IEEE_754>`_ standard for missing value handling. Because NumPy stores homogenous arrays, there has been a long standing issue in pandas where integers could not store missing records, as noted in the `pandas documentation on missing values <https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html#working-with-missing-data>`_.

Complicating factors is the presence of the ``None`` sentinel in Python, which can often times intuitively be represented as a missing value. Unfortunately ``None`` is a Python level construct, and most of the performance of the data stack in Python is implemented in lower-level C routines that cannot take advantage of this value. The Hyper API though uses this as its missing value indicator.

Because of the various missing value indicators, serializing to/from a Hyper extract via pantab cannot be lossless. On write, ``None`` and ``np.nan`` values will both be written in as missing data to the Hyper extract. On read, missing values will be read back as ``np.nan``.

Index / Columns
---------------------
A pandas DataFrame always comes with an ``Index`` used to slice / access rows. No such concept exists in Tableau, so this will be implicitly dropped when writing Hyper extracts.

With respect to columns, note that Tableau stores column labels internally as a string. You *may* be able to write non-string objects to the database (this is left to the underlying Hyper API to decide) but reading those objects back is not a lossless operation and will always return strings.


Datetime Read Performance
-------------------------

.. todo::
   Document datetime read performance degredation

Datetime Timezone Handling
--------------------------

Timezones are not supported in Hyper Extracts. Attempting to write a timezone aware array to an extract will result in an error that the dtype is not supported. The only option to write dates with timezone information would be to make the data timezone naive. You may also consider storing the timezone in a separate column as part of the extract to avoid losing information.

float32 Type Preservation
-------------------------

The Hyper API does not offer SINGLE precision storage. As such, any float32 objects will be upcast to 64 bits when writing to an extract. When reading, no attempt is made to downsize for you, so you will always get float64 back from a Hyper extract.
