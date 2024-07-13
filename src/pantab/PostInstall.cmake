# https://learn.microsoft.com/en-us/cpp/build/understanding-manifest-generation-for-c-cpp-programs?view=msvc-170#two-approaches
if(WIN32)
  execute_process("mt.exe -manifest libpantab.dll.manifest -outputresource:${SKBUILD_PROJECT_NAME}\\libpantab.dll;#2")
endif()
