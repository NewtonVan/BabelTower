file (GLOB_RECURSE srcs src/*.cpp include/*.h)
add_library(type STATIC ${srcs})
target_include_directories(type PUBLIC include)

target_link_libraries(type PUBLIC common)