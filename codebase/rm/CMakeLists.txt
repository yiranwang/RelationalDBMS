cmake_minimum_required(VERSION 3.6)
project(rm)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")



file(GLOB RM_SOURCES "*.cc")    # equivalent to set(RM_SOURCES xx1.cc xx2.cc ... )


add_library(rm ${RM_SOURCES})


add_executable(printTable printTable.cc)
target_link_libraries(printTable rbf rm)


add_executable(rmtest_create_tables  rmtest_create_tables.cc)
target_link_libraries(rmtest_create_tables rbf rm)