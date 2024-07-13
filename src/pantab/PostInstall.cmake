# https://learn.microsoft.com/en-us/cpp/build/understanding-manifest-generation-for-c-cpp-programs?view=msvc-170#two-approaches
if(WIN32)
  # Required so local installation of pantab can resolve to bundled
  # tableauhyperapi.dll and not any other on system
  # https://devblogs.microsoft.com/oldnewthing/20171011-00/?p=97195
  install(FILES libpantab.manifest DESTINATION ${SKBUILD_PROJECT_NAME}/)
  execute_process(
    COMMAND "mt.exe -manifest libpantab.manifest -outputresource:${SKBUILD_PROJECT_NAME}\\libpantab.dll;2")  # application manifest
endif()
