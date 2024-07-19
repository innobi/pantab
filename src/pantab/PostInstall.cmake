# https://learn.microsoft.com/en-us/cpp/build/understanding-manifest-generation-for-c-cpp-programs?view=msvc-170#two-approaches
if(WIN32)
  execute_process(
    COMMAND "mt.exe -manifest ${SKBUILD_PROJECT_NAME}\\tableauhyperapi.manifest -outputresource:${SKBUILD_PROJECT_NAME}\\tableauhyperapi.dll;1")
endif()
