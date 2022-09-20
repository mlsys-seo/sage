cmake_minimum_required(VERSION 3.5.1)
project(sage)

add_library(hash STATIC
        crc32.cpp
        digest.cpp
        keccak.cpp
        md5.cpp
        sha1.cpp
        sha3.cpp
        sha256.cpp)