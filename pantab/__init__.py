import pandas as pd
from tableausdk.Types import Type as ttypes
import tableausdk.HyperExtract as hpe


# pandas type in, tableau type, tab->pan type
type_mappings = (
    ('int16', ttypes.INTEGER, 'int64'),    
    ('int32', ttypes.INTEGER, 'int64'),    
    ('int64', ttypes.INTEGER, 'int64'),
    ('float32', ttypes.DOUBLE, 'float64'),    
    ('float64', ttypes.DOUBLE, 'float64'),
    ('bool', ttypes.BOOLEAN, 'bool'),
    ('datetime64[ns]', ttypes.DATETIME, 'datetime64[ns]'),
    ('timedelta64[ns]', ttypes.DURATION, 'timedelta64[ns]'),
    ('object', ttypes.UNICODE_STRING, 'object')
)


def pandas_to_tableau_type(typ):
    for ptype, ttype, _ in type_mappings:
        if typ == ptype:
            return ttype

    raise TypeError("Conversion of '{}' dtypes not yet supported!".format(typ))


def tableau_to_pandas_type(typ):
    for _, ttype, ret_type in type_mappings:
        if typ == ttype:
            return ret_type

    # Fallback to object
    return 'object'


_type_accessors = {
    ttypes.BOOLEAN: 'setBoolean',
    ttypes.DATETIME: 'setDateTime',
    ttypes.DOUBLE: 'setDouble',
    ttypes.DURATION: 'setDuration',
    ttypes.INTEGER: 'setInteger',
    ttypes.UNICODE_STRING: 'setString'
}


def frame_to_hyper(df, fn, table_nm):
    """
    Convert a DataFrame to a .hyper extract.
    """
    schema = hpe.TableDefinition()
    ttype_l = []
    for col in df.columns:
        ttype = _pandas_to_tab_type(df[col].dtype)
        ttype_l.append(ttype)
        schema.addColumn(col, ttype)

    rows = []
    accessors = tuple(_type_accessors[ttype] for ttype in ttype_l)
    for tup in df.itertuples(index=False):
        row = hpe.Row(schema)
        for index, accessor in enumerate(accessors):
            val = tup[index]
            args = [index]
            # Conditional branch can certainly be refactored, but going the
            # easy route for the time being
            if accessor == 'setDateTime':
                for window in ('year', 'month', 'day', 'hour', 'minute',
                               'second'):
                    args.append(getattr(val, window))
                # last positional arg to func must be in tenth of ms
                # will lose precision compared to pandas type
                func.args.append(val.microsecond // 100)
            elif accessor == 'setDuration':
                for window in ('day', 'hour', 'minute', 'second'):
                    args.append(getattr(val, window))
                # last positional arg to func must be in tenth of ms
                # will lose precision compared to pandas type
                args.append(val.microseconds // 100)
            else:
                args.append(val)
            getattr(row, accessor)(*args)

        rows.append(row)
    
    with hpe.Extract(fn) as extract:
        table = extract.addTable(table_nm, schema)
        for row in rows:
            table.insert(row)


def hyper_to_frame(fn):
    """
    Extracts a DataFrame from a .hyper extract.
    """
    raise NotImplementedError("Coming soon!")
