#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include "types.h"

struct ML1MSample {
    // User demographics
    std::string user_id;
    std::string gender;       // "M" or "F"
    std::string age;          // "1", "18", "25", "35", "45", "50", "56"
    std::string occupation;   // "0"-"20"
    std::string zip_code;

    // Item info
    std::string item_id;
    std::string movie_title;
    int32_t     movie_publish_year = 0;
    std::vector<std::string> movie_genres;
    int64_t     movie_rate_count = 0;
    double      movie_avg_rate = 0.0;
    int32_t     movie_hot_rank = 99999;
    int32_t     movie_genre_cnt = 0;

    // Behavior sequences: (item_id, rating) for user_movie_rate series
    std::vector<std::pair<int32_t, int32_t>> user_movie_rates;
    std::vector<std::pair<int32_t, int32_t>> user_movie_rate_1days;
    std::vector<std::pair<int32_t, int32_t>> user_movie_rate_3days;
    std::vector<std::pair<int32_t, int32_t>> user_movie_rate_7days;
    std::vector<std::pair<int32_t, int32_t>> user_movie_rate_15days;

    // User rating statistics
    int32_t user_rate_cnt = 0;
    int32_t user_rate_7day_cnt = 0;
    int32_t user_rate_15day_cnt = 0;
    int32_t user_rate_30day_cnt = 0;

    float user_rate_std = 0.0f;
    float user_rate_std_7day = 0.0f;
    float user_rate_std_15day = 0.0f;
    float user_rate_std_30day = 0.0f;

    float user_avg_rate = 3.0f;
    float user_avg_rate_7day = 3.0f;
    float user_avg_rate_15day = 3.0f;
    float user_avg_rate_30day = 3.0f;

    // Genre-level sequences
    std::vector<std::pair<std::string, float>> user_genres_rates;
    std::vector<std::pair<std::string, float>> user_genres_rate_1days;
    std::vector<std::pair<std::string, float>> user_genres_rate_3days;
    std::vector<std::pair<std::string, float>> user_genres_rate_7days;
    std::vector<std::pair<std::string, float>> user_genres_rate_15days;

    std::vector<std::pair<std::string, int32_t>> user_genres_rate_cnts;
    std::vector<std::pair<std::string, int32_t>> user_genres_rate_cnt_1days;
    std::vector<std::pair<std::string, int32_t>> user_genres_rate_cnt_3days;
    std::vector<std::pair<std::string, int32_t>> user_genres_rate_cnt_7days;
    std::vector<std::pair<std::string, int32_t>> user_genres_rate_cnt_15days;

    std::vector<std::pair<std::string, int32_t>> user_top3_genres;

    // Context
    int32_t week_day = 0;    // 1-7
    int32_t time_hour = 0;   // 0-23
    int32_t time_area = 0;   // 0-5 (= hour/4)
    int64_t time_stamp = 0;

    // Target
    int32_t target = 0;  // item_id
    int32_t label = 0;   // binary
    float   rating = 0.0f;
};
