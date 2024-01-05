nanobind_add_module(pantab NOMINSIZE pantab.cpp numpy_datetime.cpp)
target_include_directories(pantab PUBLIC ${Python_NumPy_INCLUDE_DIRS})
target_link_libraries(pantab PRIVATE Tableau::tableauhyperapi-cxx)

# TODO: would be nice to not hard code /src but CMAKE_CURRENT_LIST_DIR did not work
install(TARGETS pantab
  LIBRARY DESTINATION ${SKBUILD_PROJECT_NAME}/src)

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

# Auditwheel doesn't know how to handle the tableau libraries
# so we manually install here and exclude from auditwheel
install(FILES ${tableauhyperapi-cxx_SOURCE_DIR}/lib/${HYPERAPI_LIB_NAME}
   DESTINATION ${SKBUILD_PROJECT_NAME}/src)
install(DIRECTORY "${tableauhyperapi-cxx_SOURCE_DIR}/${HYPERAPI_BIN_LOC}/"
  DESTINATION ${SKBUILD_PROJECT_NAME}/src/hyper)
