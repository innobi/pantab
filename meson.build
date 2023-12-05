project('pantab', 'c')

py = import('python').find_installation(pure: false)

incdir_numpy = run_command(py,
  [
    '-c',
    '''
import os
import numpy as np
try:
    # Check if include directory is inside the pandas dir
    # e.g. a venv created inside the pandas dir
    # If so, convert it to a relative path
    incdir = os.path.relpath(np.get_include())
except Exception:
    incdir = np.get_include()
print(incdir)
     '''
  ],
  check: true
).stdout().strip()

inc_np = include_directories(incdir_numpy)

py.extension_module(
    'libpantab',
    ['pantab/src/pantab.c', 'pantab/src/numpy_datetime.c',
     'pantab/src/reader.c', 'pantab/src/type.c',
     'pantab/src/writer.c'],
    include_directories: [inc_np],
    install: true
)
