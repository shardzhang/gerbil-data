cmake_minimum_required(VERSION 3.10)
project(ml1m_featurizer VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(ml1m_featurizer
    main.cpp
    MurmurHash3.cpp
)

target_include_directories(ml1m_featurizer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
