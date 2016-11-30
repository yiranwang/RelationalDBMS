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

add_executable(rmtest_p0 rmtest_p0.cc)
target_link_libraries(rmtest_p0 ix rbf rm)

add_executable(rmtest_p1 rmtest_p1.cc)
target_link_libraries(rmtest_p1 ix rbf rm)

add_executable(rmtest_p2 rmtest_p2.cc)
target_link_libraries(rmtest_p2 ix rbf rm)

add_executable(rmtest_p3 rmtest_p3.cc)
target_link_libraries(rmtest_p3 ix rbf rm)

add_executable(rmtest_p4 rmtest_p4.cc)
target_link_libraries(rmtest_p4 ix rbf rm)

add_executable(rmtest_p5 rmtest_p5.cc)
target_link_libraries(rmtest_p5 ix rbf rm)

add_executable(rmtest_p6 rmtest_p6.cc)
target_link_libraries(rmtest_p6 ix rbf rm)

add_executable(rmtest_p7 rmtest_p7.cc)
target_link_libraries(rmtest_p7 ix rbf rm)

add_executable(rmtest_p8 rmtest_p8.cc)
target_link_libraries(rmtest_p8 ix rbf rm)

add_executable(rmtest_p9 rmtest_p9.cc)
target_link_libraries(rmtest_p9 ix rbf rm)