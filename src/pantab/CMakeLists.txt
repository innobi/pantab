nanobind_add_module(
  libpantab
  NOMINSIZE
  libpantab.cpp
  reader.cpp
  writer.cpp
  numpy_datetime.cpp)
target_include_directories(libpantab PUBLIC ${Python_NumPy_INCLUDE_DIRS})
target_link_libraries(libpantab
  PRIVATE Tableau::tableauhyperapi-cxx
  PRIVATE nanoarrow
)
set_target_properties(libpantab PROPERTIES CXX_CLANG_TIDY "${CLANG_TIDY_COMMAND}")
set_target_properties(nanoarrow
                      PROPERTIES POSITION_INDEPENDENT_CODE
                      ON)

install(TARGETS libpantab
  LIBRARY DESTINATION ${SKBUILD_PROJECT_NAME}/)

if(WIN32)
  set(HYPERAPI_LIB_NAME "tableauhyperapi.lib")
  set(HYPERAPI_BIN_LOC "bin/hyper")
elseif(APPLE)
  set(HYPERAPI_LIB_NAME "libtableauhyperapi.dylib")
  set(HYPERAPI_BIN_LOC "lib/hyper")
else()
  set(HYPERAPI_LIB_NAME "libtableauhyperapi.so")
  set(HYPERAPI_BIN_LOC "lib/hyper")
endif()

# Auditwheel doesn't know how to handle the cmake dependencies
# so we manually install here and exclude from auditwheel
install(FILES ${tableauhyperapi-cxx_SOURCE_DIR}/lib/${HYPERAPI_LIB_NAME}
   DESTINATION ${SKBUILD_PROJECT_NAME}/)
install(DIRECTORY "${tableauhyperapi-cxx_SOURCE_DIR}/${HYPERAPI_BIN_LOC}/"
  DESTINATION ${SKBUILD_PROJECT_NAME}/hyper)
