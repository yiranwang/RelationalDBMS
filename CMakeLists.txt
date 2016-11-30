cmake_minimum_required(VERSION 3.6)
project(project4)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")

#set(CMAKE_VERBOSE_MAKEFILE ON)



add_subdirectory(codebase/rbf)
add_subdirectory(codebase/rm)
add_subdirectory(codebase/ix)
add_subdirectory(codebase/qe)

