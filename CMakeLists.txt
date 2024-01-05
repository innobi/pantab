cmake_minimum_required(VERSION 3.18)
project(${SKBUILD_PROJECT_NAME} LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (MSVC)
else()
    add_compile_options(-Wall -Wextra)
endif()

find_package(Python COMPONENTS Interpreter Development.Module NumPy REQUIRED)

# Detect the installed nanobind package and import it into CMake
execute_process(
  COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE NB_DIR)
list(APPEND CMAKE_PREFIX_PATH "${NB_DIR}")
find_package(nanobind CONFIG REQUIRED)

if(WIN32)
  set(TABLEAU_DOWNLOAD_URL "https://downloads.tableau.com/tssoftware//tableauhyperapi-cxx-windows-x86_64-release-main.0.0.18369.r86e960ca.zip")
elseif(APPLE)
  set(TABLEAU_DOWNLOAD_URL "https://downloads.tableau.com/tssoftware//tableauhyperapi-cxx-macos-x86_64-release-main.0.0.18369.r86e960ca.zip")
else()
  set(TABLEAU_DOWNLOAD_URL "https://downloads.tableau.com/tssoftware/tableauhyperapi-cxx-linux-x86_64-release-main.0.0.18369.r86e960ca.zip")
endif()

include(FetchContent)
FetchContent_Declare(
  tableauhyperapi-cxx
  URL "${TABLEAU_DOWNLOAD_URL}"
)

FetchContent_MakeAvailable(tableauhyperapi-cxx)
list(APPEND CMAKE_PREFIX_PATH "${tableauhyperapi-cxx_SOURCE_DIR}/share/cmake")
find_package(tableauhyperapi-cxx CONFIG REQUIRED)

if (PANTAB_USE_SANITIZERS)
    add_compile_options(-fsanitize=address -fsanitize=undefined)
    add_link_options(-fsanitize=address -fsanitize=undefined)
endif()

add_subdirectory(pantab/src)
