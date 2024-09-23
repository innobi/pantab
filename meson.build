project(
    'pantab',
    'cpp',
    version: '5.0.0',
    default_options: [
        'warning_level=2',
        'cpp_std=c++20',
    ]
)

nanobind_dep = dependency('nanobind', static: true)
nanoarrow_dep = dependency('nanoarrow', static: true)
hyper_dep = dependency('tableauhyperapi')

py = import('python').find_installation(pure: false)

py.extension_module(
    'libpantab',
    'src/pantab/libpantab.cpp',
    'src/pantab/reader.cpp',
    'src/pantab/writer.cpp',
    dependencies: [nanobind_dep, nanoarrow_dep, hyper_dep],
    install: true,
    subdir: 'pantab',
)

py.install_sources(
    'src/pantab/__init__.py',
    'src/pantab/_reader.py',
    'src/pantab/_types.py',
    'src/pantab/_writer.py',
    subdir: 'pantab',
)
