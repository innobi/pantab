cmake_minimum_required(VERSION 3.20)
project(${SKBUILD_PROJECT_NAME} LANGUAGES C CXX)
set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (MSVC)
else()
    add_compile_options(-Wall -Wextra)
endif()

find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)

# Detect the installed nanobind package and import it into CMake
execute_process(
  COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE NB_DIR)
list(APPEND CMAKE_PREFIX_PATH "${NB_DIR}")
find_package(nanobind CONFIG REQUIRED)

if(WIN32)
  set(TABLEAU_DOWNLOAD_URL "https://downloads.tableau.com/tssoftware//tableauhyperapi-cxx-windows-x86_64-release-main.0.0.21200.re11c8cb9.zip")
elseif(APPLE)
  if(CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "arm64")
    set(TABLEAU_DOWNLOAD_URL "https://downloads.tableau.com/tssoftware//tableauhyperapi-cxx-macos-arm64-release-main.0.0.21200.re11c8cb9.zip")
  else()
    set(TABLEAU_DOWNLOAD_URL "https://downloads.tableau.com/tssoftware//tableauhyperapi-cxx-macos-x86_64-release-main.0.0.21200.re11c8cb9.zip")
  endif()
else()
  set(TABLEAU_DOWNLOAD_URL "https://downloads.tableau.com/tssoftware//tableauhyperapi-cxx-linux-x86_64-release-main.0.0.21200.re11c8cb9.zip")
endif()

include(FetchContent)
FetchContent_Declare(
  tableauhyperapi-cxx
  DOWNLOAD_EXTRACT_TIMESTAMP ON
  URL "${TABLEAU_DOWNLOAD_URL}"
)

FetchContent_MakeAvailable(tableauhyperapi-cxx)
list(APPEND CMAKE_PREFIX_PATH "${tableauhyperapi-cxx_SOURCE_DIR}/share/cmake")
find_package(tableauhyperapi-cxx CONFIG REQUIRED)

FetchContent_Declare(nanoarrow-project
  GIT_REPOSITORY https://github.com/apache/arrow-nanoarrow.git
  GIT_TAG 3ba38ff09c691cf9320895d04cf441ecb95caa99
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
