file (GLOB_RECURSE srcs src/*.cpp include/*.h)
add_library(common STATIC ${srcs})
target_include_directories(common PUBLIC include)

target_link_libraries(common PUBLIC buffer type)