cmake_minimum_required(VERSION 3.11)
project(asyncforest)

set(CMAKE_CXX_STANDARD 20 CACHE STRING "")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(asyncforest asyncforest.cxx)
