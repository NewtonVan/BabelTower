file (GLOB_RECURSE srcs src/*.cpp include/*.h)
add_library(buffer STATIC ${srcs})
target_include_directories(buffer PUBLIC include)

target_link_libraries(buffer PUBLIC common)