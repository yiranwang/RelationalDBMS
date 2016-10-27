cmake_minimum_required(VERSION 3.6)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")

project(ix)

file(GLOB IX_SOURCES "*.cc")

add_library(ix ${IX_SOURCES})

add_executable(ixtest_01 ixtest_01.cc)
target_link_libraries(ixtest_01 ix rbf rm)