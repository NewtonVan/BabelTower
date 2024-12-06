file (GLOB_RECURSE srcs src/*.cpp include/*.h)
add_library(storage STATIC ${srcs})
target_include_directories(storage PUBLIC include)

target_link_libraries(storage PUBLIC common)