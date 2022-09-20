cmake_minimum_required(VERSION 3.5.1)
project(sage)

include_directories(${PROJECT_SOURCE_DIR}/lib)

add_executable(converter converter.cpp)
target_link_libraries(converter ${Boost_LIBRARIES})
add_executable(sample_graph_gen sample_graph_gen.cpp)
add_executable(aggregate_sim aggregate_sim.cpp)