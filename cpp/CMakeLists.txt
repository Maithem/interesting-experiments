cmake_minimum_required(VERSION 3.12)
project(testServer)

# Set the output folder where your program will be created
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/bin)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})

# The following folder will be included
include_directories("${PROJECT_SOURCE_DIR}")

set(CMAKE_CXX_STANDARD 14)

add_executable(server ${PROJECT_SOURCE_DIR}/server.cc)

link_directories("/usr/local/lib" "/usr/lib/x86_64-linux-gnu/")

target_link_libraries(server
        wangle
        folly
        fizz
        folly
        sodium
        double-conversion
        ssl
        crypto
        glog
        gflags
        pthread
        dl
        event
        iberty
        snappy
        lz4
        lzma
        boost_context
        unwind)
