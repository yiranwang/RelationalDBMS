cmake_minimum_required(VERSION 3.6)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")

project(qe)


file(GLOB QE_SOURCES "*.cc")


add_library(qe ${QE_SOURCES})

#add_executable(rbftest1 rbftest1.cc)
#target_link_libraries(rbftest1 librbf.a)

add_executable(qetest_01 qetest_01.cc)
target_link_libraries(qetest_01 ix rbf rm qe)

add_executable(qetest_02 qetest_02.cc)
target_link_libraries(qetest_02 ix rbf rm qe)


add_executable(qetest_06 qetest_06.cc)
target_link_libraries(qetest_06 ix rbf rm qe)

add_executable(qetest_07 qetest_07.cc)
target_link_libraries(qetest_07 ix rbf rm qe)

add_executable(qetest_09 qetest_09.cc)
target_link_libraries(qetest_09 ix rbf rm qe)

add_executable(qetest_10 qetest_10.cc)
target_link_libraries(qetest_10 ix rbf rm qe)

add_executable(qetest_04 qetest_04.cc)
target_link_libraries(qetest_04 ix rbf rm qe)

add_executable(qetest_05 qetest_05.cc)
target_link_libraries(qetest_05 ix rbf rm qe)

add_executable(qetest_11 qetest_11.cc)
target_link_libraries(qetest_11 ix rbf rm qe)

add_executable(qetest_12 qetest_12.cc)
target_link_libraries(qetest_12 ix rbf rm qe)

add_executable(qetest_13 qetest_13.cc)
target_link_libraries(qetest_13 ix rbf rm qe)

add_executable(qetest_14 qetest_14.cc)
target_link_libraries(qetest_14 ix rbf rm qe)

add_executable(qetest_15 qetest_15.cc)
target_link_libraries(qetest_15 ix rbf rm qe)

add_executable(qetest_16 qetest_16.cc)
target_link_libraries(qetest_16 ix rbf rm qe)

add_executable(qetest_08 qetest_08.cc)
target_link_libraries(qetest_08 ix rbf rm qe)

add_executable(qetest_03 qetest_03.cc)
target_link_libraries(qetest_03 ix rbf rm qe)