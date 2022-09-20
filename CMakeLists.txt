cmake_minimum_required(VERSION 3.5.1)
project(sage)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -O3 -g" )
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -DNDEBUG" )

include_directories(${Boost_INCLUDE_DIR})
add_definitions( -DBOOST_ALL_NO_LIB )
set( Boost_USE_STATIC_LIBS ON )
find_package(Boost REQUIRED COMPONENTS program_options log system)

string(TIMESTAMP VERSION "%Y-%m-%d %H:%M")
add_definitions(-DVERSION="${VERSION}")

link_libraries(pthread ${Boost_LIBRARIES} aio)

include_directories("${PROJECT_SOURCE_DIR}" lib algs tools)
add_subdirectory("${PROJECT_SOURCE_DIR}/lib/unit-test")
add_subdirectory("${PROJECT_SOURCE_DIR}/tools")
add_subdirectory("${PROJECT_SOURCE_DIR}/algs")
