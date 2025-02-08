cmake_minimum_required(VERSION 3.6)

project(Broker VERSION 1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include_directories(${CROW_INCLUDE_DIR})

message("$ENV{HOME}/Repository/vcpkg/scripts/buildsystems/vcpkg.cmake")


set(CMAKE_PREFIX_PATH "$ENV{HOME}/Repository/vcpkg/installed/arm64-osx/share")

set(GFLAGS_USE_TARGET_NAMESPACE ON)
find_package(gflags CONFIG REQUIRED)

# Find the Crow package
find_package(Crow CONFIG REQUIRED)

# Find the Folly package
find_package(Folly CONFIG REQUIRED)



add_executable(Broker server.cpp)

# Link the crow library
target_link_libraries(Broker PRIVATE Crow::Crow)
# Link the folly library

# target_link_libraries(Broker PRIVATE gflags::gflags)
target_link_libraries(Broker PRIVATE Folly::folly)