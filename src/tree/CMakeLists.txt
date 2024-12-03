file (GLOB_RECURSE srcs src/*.cpp include/*.h)
add_library(tree STATIC ${srcs})
target_include_directories(tree PUBLIC include)

target_link_libraries(tree PUBLIC buffer)