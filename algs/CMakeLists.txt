cmake_minimum_required(VERSION 3.5.1)
project(sage)

include_directories(${PROJECT_SOURCE_DIR})

add_executable(bfs bfs.cpp)
add_executable(knn knn.cpp)
add_executable(kcore kcore.cpp)
add_executable(topk topk.cpp)
add_executable(sp shortest_path.cpp)
add_executable(pr pagerank.cpp)
add_executable(cl clustering.cpp)
add_executable(im influence_maximization.cpp)
