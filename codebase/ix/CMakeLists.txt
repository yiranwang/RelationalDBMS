cmake_minimum_required(VERSION 3.6)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")

project(ix)


file(GLOB IX_SOURCES "*.cc")


add_library(ix ${IX_SOURCES})



add_executable(ixtest_01 ixtest_01.cc)
target_link_libraries(ixtest_01 ix rbf rm)

add_executable(ixtest_02 ixtest_02.cc)
target_link_libraries(ixtest_02 ix rbf rm)

add_executable(ixtest_03 ixtest_03.cc)
target_link_libraries(ixtest_03 ix rbf rm)

add_executable(ixtest_06 ixtest_06.cc)
target_link_libraries(ixtest_06 ix rbf rm)

add_executable(ixtest_08 ixtest_08.cc)
target_link_libraries(ixtest_08 ix rbf rm)

add_executable(ixtest_09 ixtest_09.cc)
target_link_libraries(ixtest_09 ix rbf rm)

add_executable(ixtest_10 ixtest_10.cc)
target_link_libraries(ixtest_10 ix rbf rm)

add_executable(ixtest_11 ixtest_11.cc)
target_link_libraries(ixtest_11 ix rbf rm)

add_executable(ixtest_12 ixtest_12.cc)
target_link_libraries(ixtest_12 ix rbf rm)

add_executable(ixtest_13 ixtest_13.cc)
target_link_libraries(ixtest_13 ix rbf rm)