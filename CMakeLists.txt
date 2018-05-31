cmake_minimum_required(VERSION 3.8)
project(os_ex3)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS -lpthread)


set(SOURCE_FILES SampleClient.cpp MapReduceFramework.h MapReduceClient.h MapReduceFramework.cpp Barrier.cpp Barrier.h)
add_executable(os_ex3 ${SOURCE_FILES})