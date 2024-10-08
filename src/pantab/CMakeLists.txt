set(PANTAB_SOURCES
  libpantab.cpp
  reader.cpp
  writer.cpp
)

nanobind_add_module(
  libpantab
  NOMINSIZE
  ${PANTAB_SOURCES}
)
target_link_libraries(libpantab
  PRIVATE Tableau::tableauhyperapi-cxx
  PRIVATE nanoarrow
)
set_target_properties(nanoarrow
                      PROPERTIES POSITION_INDEPENDENT_CODE
                      ON)

if(WIN32)
  set(HYPERAPI_LIB_DIR "bin")
  set(HYPERAPI_LIB_NAME "tableauhyperapi.dll")
  set(HYPERAPI_BIN_LOC "bin/hyper")
elseif(APPLE)
  set(HYPERAPI_LIB_DIR "lib")
  set(HYPERAPI_LIB_NAME "libtableauhyperapi.dylib")
  set(HYPERAPI_BIN_LOC "lib/hyper")
set_target_properties(
  libpantab
  PROPERTIES
  INSTALL_RPATH "@loader_path"
)
else()
  set(HYPERAPI_LIB_DIR "lib")
  set(HYPERAPI_LIB_NAME "libtableauhyperapi.so")
  set(HYPERAPI_BIN_LOC "lib/hyper")
set_target_properties(
  libpantab
  PROPERTIES
  INSTALL_RPATH "$ORIGIN"
)
endif()

install(TARGETS libpantab
  LIBRARY DESTINATION ${SKBUILD_PROJECT_NAME}/)

# Auditwheel doesn't know how to handle the cmake dependencies
# so we manually install here and exclude from auditwheel
install(FILES ${tableauhyperapi-cxx_SOURCE_DIR}/${HYPERAPI_LIB_DIR}/${HYPERAPI_LIB_NAME}
   DESTINATION ${SKBUILD_PROJECT_NAME}/)
install(DIRECTORY "${tableauhyperapi-cxx_SOURCE_DIR}/${HYPERAPI_BIN_LOC}/"
  DESTINATION ${SKBUILD_PROJECT_NAME}/hyper
  USE_SOURCE_PERMISSIONS
)

# Support out of source builds
set(SRC_FILES
  __init__.py
  _reader.py
  _types.py
  _writer.py
)

if (NOT (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_CURRENT_BINARY_DIR) OR MSVC)
  if (MSVC)
    set(OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>)
  else()
    set(OUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
  endif()

  foreach(SRC_FILE IN LISTS SRC_FILES)
    set(IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/${SRC_FILE})
    set(OUT_FILE ${OUT_DIR}/${SRC_FILE})
    set(SRC_FILES_OUT ${SRC_FILES_OUT} ${OUT_FILE})
    add_custom_command(
      DEPENDS ${IN_FILE} TARGET OUTPUT ${OUT_FILE}
      COMMAND ${CMAKE_COMMAND} -E copy_if_different ${IN_FILE} ${OUT_DIR})
  endforeach()

  add_custom_target(copy-python-src ALL DEPENDS ${SRC_FILES_OUT})
endif()
