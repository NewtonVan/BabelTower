file (GLOB_RECURSE srcs src/*.cpp include/*.h)
add_executable(babel_tower ${srcs})

target_link_libraries(babel_tower PUBLIC tree aio pthread)
target_include_directories(babel_tower PUBLIC include)