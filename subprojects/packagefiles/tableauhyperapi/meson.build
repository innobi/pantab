project('tableauhyperapi')

compiler = meson.get_compiler('cpp')
incdir = include_directories('include')

hyper_dep = declare_dependency(
    dependencies: [compiler.find_library('tableauhyperapi', dirs: meson.current_source_dir() / 'lib')],
    include_directories: [incdir],
)

# Tableau ships a pre-built executable and binary we need
# in our build. Using custom_target gets us there reasonably
py = import('python').find_installation(pure: false)

if build_machine.system() == 'windows'
  hyper_exe = files('bin/hyper/hyperd.exe')
  install_data(hyper_exe, install_dir: py.get_install_dir(pure: false) / 'pantab.libs')
else
  hyper_exe = files('lib/hyper/hyperd')
  install_data(hyper_exe, install_dir: py.get_install_dir(pure: false) / 'pantab.libs')
endif
