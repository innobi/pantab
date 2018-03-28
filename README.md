# pantab: Connecting pandas with Tableau

## What is it?

**pantab** is a tool to help generate Hyper extracts (available with Tableau 10.5+) from a pandas DataFrame.


```python
import pandas as pd
import pantab

df = pd.DataFrame(
    [[1, 2, 3, 4., 5., True, pd.to_datetime('1/1/18'), 'foo'],
     [6, 7, 8, 9., 10., True, pd.to_datetime('1/1/19'), 'foo']
     ], columns=['foo', 'bar', 'baz', 'qux', 'quux', 'quuuz', 'corge',
                 'garply'])

pantab.frame_to_hyper(df, 'foo.hyper')
```

The above will generate a Hyper extract in the specified location, which you can then open in Tableau.

![Hyper Extract in Tableau](samples/demo.png)

## Requirements & Installation

* Python >= 3.5
* Tableau Extract API (see installation instructions [here](https://onlinehelp.tableau.com/v10.5/api/extract_api/en-us/help.htm#Extract/extract_api_using_python.htm))
* [pandas](https://pandas.pydata.org)

The preferred way to install this package is from pip. Note that this will resolve the pandas dependency for you but **will not resolve the Tableau Extract API** dependency. Please refer to their installation instructions.

```sh
pip install pantab
```

If you want to run the test suite for this application, you will also need to install [pytest](https://pytest.org).

## Known Limitations

The following are all known limitations of this tool:

* The same Hyper extract cannot be written to twice
* You can write Hyper extracts, but cannot currently read them
* You cannot change the table name in your extract (defaulted to 'Extract' in SDK)
* Duration, Categorical, Complex and Time-zone aware dtypes cannot be written to a Hyper extract

Also note that Tableau does not expose an API to manage different size types. int and float types will all be upcast to 64 bit. While this won't make a difference for writing files, it could prevent the preservation of type size when round-tripping if a reader can be supported by the Tableau API in the future.

## Contributing

Want to make this package better? Awesome - any and all contributions are appreciated! With that said, any code contributions **must** contain test cases to be considered for merging.