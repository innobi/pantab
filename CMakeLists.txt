cmake_minimum_required(VERSION 3.18)
project(${SKBUILD_PROJECT_NAME} LANGUAGES C CXX)
set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (MSVC)
else()
    add_compile_options(-Wall -Wextra -Werror)
endif()

find_package(Python COMPONENTS Interpreter Development.Module NumPy REQUIRED)

# Detect the installed nanobind package and import it into CMake
execute_process(
  COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE NB_DIR)
list(APPEND CMAKE_PREFIX_PATH "${NB_DIR}")
find_package(nanobind CONFIG REQUIRED)

if(WIN32)
  # The tableauhyperapi.dll on windows needs to be patched to be used in a SxS application
  # to change the manifest and make it an assembly, rather than an application
  # without this, Windows can easily mistake the vendored tableauhyperapi for
  # another on the system, and since they are not binary compatible that leads easily
  # to segmentation faults
  # to fix on windows, you must download the hyper api, unpack it, then run
  # mt.exe -manifest <PANTAB_ROOT>\src\pantab\tableauhyperapi.manifest -outputresource:<HYPER_ROOT>\bin\tableauhyperapi.dll;1
  # before repackaging
  # note that
  # mt -inputresource:tableauhyperapi.dll;1 -out:extracted.manifest
  # may be helpful to ensure the proper manifest was embedded into tableauhyperapi.dll
  set(TABLEAU_DOWNLOAD_URL "https://www.dropbox.com/scl/fi/kjjxrgnhfczzkanv3kaf1/tableauhyperapi-cxx-windows-x86_64-release-main.0.0.18618.r6580261c.zip?rlkey=c1iy6lrixes2vtwbrhxzclboz&st=y1om0fky&dl=1")
elseif(APPLE)
  set(TABLEAU_DOWNLOAD_URL "https://downloads.tableau.com/tssoftware//tableauhyperapi-cxx-macos-x86_64-release-main.0.0.18618.r6580261c.zip")
else()
  set(TABLEAU_DOWNLOAD_URL "https://downloads.tableau.com/tssoftware//tableauhyperapi-cxx-linux-x86_64-release-main.0.0.18618.r6580261c.zip")
endif()

include(FetchContent)
FetchContent_Declare(
  tableauhyperapi-cxx
  URL "${TABLEAU_DOWNLOAD_URL}"
  DOWNLOAD_EXTRACT_TIMESTAMP ON
)

FetchContent_MakeAvailable(tableauhyperapi-cxx)
list(APPEND CMAKE_PREFIX_PATH "${tableauhyperapi-cxx_SOURCE_DIR}/share/cmake")
find_package(tableauhyperapi-cxx CONFIG REQUIRED)

FetchContent_Declare(nanoarrow-project
  GIT_REPOSITORY https://github.com/apache/arrow-nanoarrow.git
  GIT_TAG dab87aaea4c2c05d24b745d58e50726bd0553452
)
FetchContent_MakeAvailable(nanoarrow-project)

if (PANTAB_USE_SANITIZERS)
    add_compile_options(-fsanitize=address -fsanitize=undefined)
    add_link_options(-fsanitize=address -fsanitize=undefined)
endif()

find_program(CLANG_TIDY_EXE NAMES "clang-tidy")
if (CLANG_TIDY_EXE_FOUND)
  set(CLANG_TIDY_COMMAND "${CLANG_TIDY_EXE}" "-checks=-*,modernize-*")
else()
  message("Could not find clang-tidy installation - checks disabled")
endif()

add_subdirectory(src/pantab)
