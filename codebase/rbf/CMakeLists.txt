cmake_minimum_required(VERSION 3.6)
project(rbf)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")



#set(LIBRBF_SOURCES pfm.cc rbfm.cc rbfm_aux.cc rbfm_scan.cc ) # Setup the list of sources here.

file(GLOB RBF_SOURCES "*.cc")

add_library(rbf ${RBF_SOURCES})


#add_executable(rbftest1 rbftest1.cc)
#target_link_libraries(rbftest1 librbf.a)