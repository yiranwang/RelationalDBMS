cmake_minimum_required(VERSION 3.6)
project(rm)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")



file(GLOB RM_SOURCES "*.cc")    # equivalent to set(RM_SOURCES xx1.cc xx2.cc ... )


add_library(rm ${RM_SOURCES})


#[[
add_executable(printTable printTable.cc)
target_link_libraries(printTable ix rbf rm)]]


add_executable(rmtest_create_tables rmtest_create_tables.cc)
target_link_libraries(rmtest_create_tables ix rbf rm)

add_executable(rmtest_delete_tables rmtest_delete_tables.cc)
target_link_libraries(rmtest_delete_tables ix rbf rm)

add_executable(rmtest_00 rmtest_00.cc)
target_link_libraries(rmtest_00 ix rbf rm)

add_executable(rmtest_01 rmtest_01.cc)
target_link_libraries(rmtest_01 ix rbf rm)

add_executable(rmtest_02 rmtest_02.cc)
target_link_libraries(rmtest_02 ix rbf rm)

add_executable(rmtest_03 rmtest_03.cc)
target_link_libraries(rmtest_03 ix rbf rm)

add_executable(rmtest_04 rmtest_04.cc)
target_link_libraries(rmtest_04 ix rbf rm)

add_executable(rmtest_05 rmtest_05.cc)
target_link_libraries(rmtest_05 ix rbf rm)

add_executable(rmtest_06 rmtest_06.cc)
target_link_libraries(rmtest_06 ix rbf rm)

add_executable(rmtest_07 rmtest_07.cc)
target_link_libraries(rmtest_07 ix rbf rm)

add_executable(rmtest_08 rmtest_08.cc)
target_link_libraries(rmtest_08 ix rbf rm)

add_executable(rmtest_09 rmtest_09.cc)
target_link_libraries(rmtest_09 ix rbf rm)

add_executable(rmtest_10 rmtest_10.cc)
target_link_libraries(rmtest_10 ix rbf rm)

add_executable(rmtest_11 rmtest_11.cc)
target_link_libraries(rmtest_11 ix rbf rm)

add_executable(rmtest_12 rmtest_12.cc)
target_link_libraries(rmtest_12 ix rbf rm)

add_executable(rmtest_13 rmtest_13.cc)
target_link_libraries(rmtest_13 ix rbf rm)

add_executable(rmtest_13b rmtest_13b.cc)
target_link_libraries(rmtest_13b ix rbf rm)

add_executable(rmtest_14 rmtest_14.cc)
target_link_libraries(rmtest_14 ix rbf rm)

add_executable(rmtest_15 rmtest_15.cc)
target_link_libraries(rmtest_15 ix rbf rm)
