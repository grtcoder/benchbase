cmake_minimum_required(VERSION 3.12)
project(Server Version 1.0)

# Add your source files here
set(SOURCES
    src/main.cpp
)

# Add your include directories here
include_directories(
    include
)

# Add your target executable
add_executable(Server ${SOURCES})