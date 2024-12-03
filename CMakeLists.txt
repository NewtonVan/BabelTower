cmake_minimum_required(VERSION 3.20)
project(babel_tower_project)

set(CMAKE_CXX_STANDARD 20)
message(STATUS "CXX Compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "C Compiler: ${CMAKE_C_COMPILER}")


# Set different flags for Debug and Release modes
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fnon-call-exceptions -fasynchronous-unwind-tables")
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3 -fnon-call-exceptions -fasynchronous-unwind-tables")

add_subdirectory(bench)
add_subdirectory(src)

# clean 
add_custom_target(clean_bb
    COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_BINARY_DIR}/babel_tower     # Removes vmcache in the build directory
    COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_SOURCE_DIR}/babel_tower     # Removes vmcache in the current directory
)

# Set the install prefix to the current directory
set(CMAKE_INSTALL_PREFIX ${CMAKE_SOURCE_DIR})

# Install the babel_tower executable to the current directory
install(TARGETS babel_tower
    RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}  # Place executable in the build directory
)