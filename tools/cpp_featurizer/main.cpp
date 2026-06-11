#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>
#include "types.h"
#include "sample.h"
#include "feature.h"
#include "featurizer.h"

// ---------------------------------------------------------------------------
// Build a sample manually (for golden data validation)
// ---------------------------------------------------------------------------
ML1MSample build_test_sample() {
    ML1MSample s;
    s.user_id = "1";
    s.gender = "M";
    s.age = "25";
    s.occupation = "4";
    s.zip_code = "12345";

    s.item_id = "1193";
    s.movie_title = "One Flew Over the Cuckoo's Nest (1975)";
    s.movie_publish_year = 1975;
    s.movie_genres = {"drama"};
    s.movie_rate_count = 200;
    s.movie_avg_rate = 4.2;
    s.movie_hot_rank = 50;
    s.movie_genre_cnt = 1;

    s.user_movie_rates       = {{1193, 5}, {1197, 3}, {1198, 4}};
    s.user_movie_rate_1days  = {{1193, 5}};
    s.user_movie_rate_3days  = {{1193, 5}, {1197, 3}};
    s.user_movie_rate_7days  = {{1193, 5}, {1197, 3}, {1198, 4}};
    s.user_movie_rate_15days = {{1193, 5}, {1197, 3}, {1198, 4}};

    s.user_rate_cnt = 3;
    s.user_rate_7day_cnt = 3;
    s.user_rate_15day_cnt = 3;
    s.user_rate_30day_cnt = 3;
    s.user_rate_std = 1.0f;
    s.user_rate_std_7day = 1.0f;
    s.user_rate_std_15day = 1.0f;
    s.user_rate_std_30day = 1.0f;
    s.user_avg_rate = 4.0f;
    s.user_avg_rate_7day = 4.0f;
    s.user_avg_rate_15day = 4.0f;
    s.user_avg_rate_30day = 4.0f;

    s.user_genres_rates       = {{"drama", 4.5f}, {"comedy", 3.5f}};
    s.user_genres_rate_1days  = {{"drama", 4.5f}};
    s.user_genres_rate_3days  = {{"drama", 4.5f}, {"comedy", 3.5f}};
    s.user_genres_rate_7days  = {{"drama", 4.5f}, {"comedy", 3.5f}};
    s.user_genres_rate_15days = {{"drama", 4.5f}, {"comedy", 3.5f}};

    s.user_genres_rate_cnts       = {{"drama", 5}, {"comedy", 3}};
    s.user_genres_rate_cnt_1days  = {{"drama", 2}};
    s.user_genres_rate_cnt_3days  = {{"drama", 2}, {"comedy", 1}};
    s.user_genres_rate_cnt_7days  = {{"drama", 4}, {"comedy", 2}};
    s.user_genres_rate_cnt_15days = {{"drama", 5}, {"comedy", 3}};

    s.user_top3_genres = {{"drama", 5}, {"comedy", 3}, {"action", 1}};

    s.week_day = 3;
    s.time_hour = 14;
    s.time_area = 3;
    s.time_stamp = 1700000000000L;

    s.target = 1193;
    s.rating = 5.0f;
    s.label = 1;

    return s;
}

// ---------------------------------------------------------------------------
// Register all 58 enabled features
// ---------------------------------------------------------------------------
void register_all_features(ML1MFeaturizer& f) {
    // User demographics
    f.register_feature(new UserAge(2, "user_age"));
    f.register_feature(new UserGender(3, "user_gender"));
    f.register_feature(new UserOccupation(4, "user_occupation"));

    // User rate std (bucketed)
    f.register_feature(new UserRateStd(6, "user_rate_std"));
    f.register_feature(new UserRateStd7Day(7, "user_rate_std_7day"));
    f.register_feature(new UserRateStd15Day(8, "user_rate_std_15day"));
    f.register_feature(new UserRateStd30Day(9, "user_rate_std_30day"));

    // User rate std (continuous)
    f.register_feature(new UserRateStdContinue(18, "user_rate_std_continue"));
    f.register_feature(new UserRateStd7DayContinue(19, "user_rate_std_7day_continue"));
    f.register_feature(new UserRateStd15DayContinue(21, "user_rate_std_15day_continue"));
    f.register_feature(new UserRateStd30DayContinue(22, "user_rate_std_30day_continue"));

    // User movie rate cnt (bucketed)
    f.register_feature(new UserMovieRateCnt(10, "user_movie_rate_cnt"));
    f.register_feature(new UserMovieRateCnt7Day(11, "user_movie_rate_cnt_7day"));
    f.register_feature(new UserMovieRateCnt15Day(12, "user_movie_rate_cnt_15day"));
    f.register_feature(new UserMovieRateCnt30Day(13, "user_movie_rate_cnt_30day"));

    // User avg rate (bucketed)
    f.register_feature(new UserAvgRate(14, "user_avg_rate"));
    f.register_feature(new UserAvgRate7Day(15, "user_avg_rate_7day"));
    f.register_feature(new UserAvgRate15Day(16, "user_avg_rate_15day"));
    f.register_feature(new UserAvgRate30Day(17, "user_avg_rate_30day"));

    // User avg rate (continuous)
    f.register_feature(new UserAvgRateContinue(23, "user_avg_rate_continue"));
    f.register_feature(new UserAvgRate7DayContinue(24, "user_avg_rate_7day_continue"));
    f.register_feature(new UserAvgRate15DayContinue(25, "user_avg_rate_15day_continue"));
    f.register_feature(new UserAvgRate30DayContinue(26, "user_avg_rate_30day_continue"));

    // Item features
    f.register_feature(new MovieTitle(102, "movie_title"));
    f.register_feature(new MovieGenres(103, "movie_genres"));
    f.register_feature(new MovieRateCount(104, "movie_rate_count"));
    f.register_feature(new MovieAvgRate(105, "movie_avg_rate"));
    f.register_feature(new MovieGenreCnt(106, "movie_genre_cnt"));
    f.register_feature(new MovieHotRank(107, "item_hot_rank"));
    f.register_feature(new MoviePublishYear(108, "movie_publish_year"));
    f.register_feature(new MovieAvgRateContinue(109, "movie_avg_rate_continue"));

    // Context features
    f.register_feature(new ContextTimeHour(201, "context_time_hour"));
    f.register_feature(new ContextTimeArea(202, "context_time_area"));
    f.register_feature(new ContextTimeWeek(203, "context_time_week"));
    f.register_feature(new IsWeekend(204, "context_is_weekend"));

    // User movie rate sequences
    f.register_feature(new UserMovieRate(301, "user_movie_rate"));
    f.register_feature(new UserMovieRate1Day(302, "user_movie_rate_1day"));
    f.register_feature(new UserMovieRate3Day(303, "user_movie_rate_3day"));
    f.register_feature(new UserMovieRate7Day(304, "user_movie_rate_7day"));
    f.register_feature(new UserMovieRate15Day(305, "user_movie_rate_15day"));

    // User genre rate sequences
    f.register_feature(new UserGenresRate(306, "user_genres_rate"));
    f.register_feature(new UserGenresRate1Day(307, "user_genres_rate_1day"));
    f.register_feature(new UserGenresRate3Day(308, "user_genres_rate_3day"));
    f.register_feature(new UserGenresRate7Day(309, "user_genres_rate_7day"));
    f.register_feature(new UserGenresRate15Day(310, "user_genres_rate_15day"));

    // User genre rate counts
    f.register_feature(new UserGenresRateCnts(312, "user_genres_rate_cnts"));
    f.register_feature(new UserGenresRateCnt1Days(313, "user_genres_rate_cnt_1days"));
    f.register_feature(new UserGenresRateCnt3Days(314, "user_genres_rate_cnt_3days"));
    f.register_feature(new UserGenresRateCnt7Days(315, "user_genres_rate_cnt_7days"));
    f.register_feature(new UserGenresRateCnt15Days(316, "user_genres_rate_cnt_15days"));

    // User top genres
    f.register_feature(new UserTop3Genres(317, "user_top3_genres"));

    // User watch same genre
    f.register_feature(new UserWatchSameGenre(351, "user_watch_same_genre"));
    f.register_feature(new UserWatchSameGenre1Day(352, "user_watch_same_genre_1day"));
    f.register_feature(new UserWatchSameGenre3Day(353, "user_watch_same_genre_3day"));
    f.register_feature(new UserWatchSameGenre7Day(354, "user_watch_same_genre_7day"));
    f.register_feature(new UserWatchSameGenre15Day(355, "user_watch_same_genre_15day"));

    // User same genre avg rate
    f.register_feature(new UserSameGenreAvgRate(356, "user_same_genre_avg_rate"));
    f.register_feature(new UserSameGenreAvgRateContinue(357, "user_same_genre_avg_rate_continue"));
}

// ---------------------------------------------------------------------------
// Register all 22 cross features (all disabled in yaml; registered for testing)
// Each cross feature owns separate constituent instances to avoid side effects.
// ---------------------------------------------------------------------------
void register_all_cross_features(ML1MFeaturizer& f) {
    // -- 2-way crosses --
    // movie_genres_xx_user_genres_rate (401)
    {
    auto* cf = new CrossFeature(401, "movie_genres_xx_user_genres_rate");
      cf->add_constituent(new MovieGenres(103, "movie_genres"));
      cf->add_constituent(new UserGenresRate(306, "user_genres_rate"));
      f.register_cross(cf);
      }

    // movie_genres_xx_user_genres_rate_1day (402)
    {
    auto* cf = new CrossFeature(402, "movie_genres_xx_user_genres_rate_1day");
      cf->add_constituent(new MovieGenres(103, "movie_genres"));
      cf->add_constituent(new UserGenresRate1Day(307, "user_genres_rate_1day"));
      f.register_cross(cf);
      }

    // movie_publish_year_xx_user_age (406)
    { auto* cf = new CrossFeature(406, "movie_publish_year_xx_user_age");
      cf->add_constituent(new MoviePublishYear(108, "movie_publish_year"));
      cf->add_constituent(new UserAge(2, "user_age"));
      f.register_cross(cf); }

    // movie_rate_count_xx_user_rate_std (410)
    { auto* cf = new CrossFeature(410, "movie_rate_count_xx_user_rate_std");
      cf->add_constituent(new MovieRateCount(104, "movie_rate_count"));
      cf->add_constituent(new UserRateStd(6, "user_rate_std"));
      f.register_cross(cf); }

    // movie_hot_rank_xx_user_avg_rate (411)
    { auto* cf = new CrossFeature(411, "movie_hot_rank_xx_user_avg_rate");
      cf->add_constituent(new MovieHotRank(107, "item_hot_rank"));
      cf->add_constituent(new UserAvgRate(14, "user_avg_rate"));
      f.register_cross(cf); }

    // movie_publish_year_xx_user_avg_rate (412)
    { auto* cf = new CrossFeature(412, "movie_publish_year_xx_user_avg_rate");
      cf->add_constituent(new MoviePublishYear(108, "movie_publish_year"));
      cf->add_constituent(new UserAvgRate(14, "user_avg_rate"));
      f.register_cross(cf); }

    // movie_genre_cnt_xx_user_avg_rate (413)
    { auto* cf = new CrossFeature(413, "movie_genre_cnt_xx_user_avg_rate");
      cf->add_constituent(new MovieGenreCnt(106, "movie_genre_cnt"));
      cf->add_constituent(new UserAvgRate(14, "user_avg_rate"));
      f.register_cross(cf); }

    // movie_hot_rank_xx_user_genre_avg_rate (414)
    { auto* cf = new CrossFeature(414, "movie_hot_rank_xx_user_genre_avg_rate");
      cf->add_constituent(new MovieHotRank(107, "item_hot_rank"));
      cf->add_constituent(new UserSameGenreAvgRate(356, "user_same_genre_avg_rate"));
      f.register_cross(cf); }

    // movie_genres_xx_user_gender (417)
    { auto* cf = new CrossFeature(417, "movie_genres_xx_user_gender");
      cf->add_constituent(new MovieGenres(103, "movie_genres"));
      cf->add_constituent(new UserGender(3, "user_gender"));
      f.register_cross(cf); }

    // movie_genres_xx_user_occupation (418)
    { auto* cf = new CrossFeature(418, "movie_genres_xx_user_occupation");
      cf->add_constituent(new MovieGenres(103, "movie_genres"));
      cf->add_constituent(new UserOccupation(4, "user_occupation"));
      f.register_cross(cf); }

    // movie_genres_xx_user_age (419)
    { auto* cf = new CrossFeature(419, "movie_genres_xx_user_age");
      cf->add_constituent(new MovieGenres(103, "movie_genres"));
      cf->add_constituent(new UserAge(2, "user_age"));
      f.register_cross(cf); }

    // movie_genres_xx_is_weekend (450)
    { auto* cf = new CrossFeature(450, "movie_genres_xx_is_weekend");
      cf->add_constituent(new MovieGenres(103, "movie_genres"));
      cf->add_constituent(new IsWeekend(204, "context_is_weekend"));
      f.register_cross(cf); }

    // movie_hot_rank_xx_is_weekend (451)
    { auto* cf = new CrossFeature(451, "movie_hot_rank_xx_is_weekend");
      cf->add_constituent(new MovieHotRank(107, "item_hot_rank"));
      cf->add_constituent(new IsWeekend(204, "context_is_weekend"));
      f.register_cross(cf); }

    // user_age_xx_is_weekend (452)
    { auto* cf = new CrossFeature(452, "user_age_xx_is_weekend");
      cf->add_constituent(new UserAge(2, "user_age"));
      cf->add_constituent(new IsWeekend(204, "context_is_weekend"));
      f.register_cross(cf); }

    // user_gender_xx_context_time_hour (453)
    { auto* cf = new CrossFeature(453, "user_gender_xx_context_time_hour");
      cf->add_constituent(new UserGender(3, "user_gender"));
      cf->add_constituent(new ContextTimeHour(201, "context_time_hour"));
      f.register_cross(cf); }

    // -- 3-way crosses --
    // movie_genres_xx_user_age_xx_user_gender (460)
    { auto* cf = new CrossFeature(460, "movie_genres_xx_user_age_xx_user_gender");
      cf->add_constituent(new MovieGenres(103, "movie_genres"));
      cf->add_constituent(new UserAge(2, "user_age"));
      cf->add_constituent(new UserGender(3, "user_gender"));
      f.register_cross(cf); }

    // movie_genres_xx_user_gender_xx_user_occupation (461)
    { auto* cf = new CrossFeature(461, "movie_genres_xx_user_gender_xx_user_occupation");
      cf->add_constituent(new MovieGenres(103, "movie_genres"));
      cf->add_constituent(new UserGender(3, "user_gender"));
      cf->add_constituent(new UserOccupation(4, "user_occupation"));
      f.register_cross(cf); }

    // movie_publish_year_xx_user_age_xx_user_occupation (462)
    { auto* cf = new CrossFeature(462, "movie_publish_year_xx_user_age_xx_user_occupation");
      cf->add_constituent(new MoviePublishYear(108, "movie_publish_year"));
      cf->add_constituent(new UserAge(2, "user_age"));
      cf->add_constituent(new UserOccupation(4, "user_occupation"));
      f.register_cross(cf); }

    // movie_avg_rate_xx_movie_hot_rank_xx_user_avg_rate (463)
    { auto* cf = new CrossFeature(463, "movie_avg_rate_xx_movie_hot_rank_xx_user_avg_rate");
      cf->add_constituent(new MovieAvgRate(105, "movie_avg_rate"));
      cf->add_constituent(new MovieHotRank(107, "item_hot_rank"));
      cf->add_constituent(new UserAvgRate(14, "user_avg_rate"));
      f.register_cross(cf); }

    // movie_genre_cnt_xx_user_total_rate_cnt_xx_user_avg_rate (465)
    { auto* cf = new CrossFeature(465, "movie_genre_cnt_xx_user_total_rate_cnt_xx_user_avg_rate");
      cf->add_constituent(new MovieGenreCnt(106, "movie_genre_cnt"));
      cf->add_constituent(new UserMovieRateCnt(10, "user_movie_rate_cnt"));
      cf->add_constituent(new UserAvgRate(14, "user_avg_rate"));
      f.register_cross(cf); }

    // movie_genre_cnt_xx_movie_hot_rank_xx_user_genre_avg_rate (466)
    { auto* cf = new CrossFeature(466, "movie_genre_cnt_xx_movie_hot_rank_xx_user_genre_avg_rate");
      cf->add_constituent(new MovieGenreCnt(106, "movie_genre_cnt"));
      cf->add_constituent(new MovieHotRank(107, "item_hot_rank"));
      cf->add_constituent(new UserSameGenreAvgRate(356, "user_same_genre_avg_rate"));
      f.register_cross(cf); }

    // movie_publish_year_xx_movie_avg_rate_xx_user_avg_rate (467)
    { auto* cf = new CrossFeature(467, "movie_publish_year_xx_movie_avg_rate_xx_user_avg_rate");
      cf->add_constituent(new MoviePublishYear(108, "movie_publish_year"));
      cf->add_constituent(new MovieAvgRate(105, "movie_avg_rate"));
      cf->add_constituent(new UserAvgRate(14, "user_avg_rate"));
      f.register_cross(cf); }
}

// ---------------------------------------------------------------------------
// Print results as CSV (matching DumpGoldenData.scala output format)
// ---------------------------------------------------------------------------
void print_csv_header() {
    std::cout << "feature_name,field_index,field_type,raw,feature_id,hash,value\n";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    ML1MFeaturizer featurizer;
    register_all_features(featurizer);
    register_all_cross_features(featurizer);

    std::string pos_map_path;
    if (argc >= 2) {
        pos_map_path = argv[1];
    }

    if (!pos_map_path.empty()) {
        if (featurizer.load_pos_map(pos_map_path)) {
            std::cerr << "Loaded pos_map with " << featurizer.pos_map.size()
                      << " features, " << featurizer.target_map.size()
                      << " targets, timestamp=" << featurizer.pos_map_timestamp << "\n";
        } else {
            std::cerr << "WARNING: Could not load pos_map from " << pos_map_path << "\n";
        }
    }

    ML1MSample sample = build_test_sample();

    print_csv_header();
    for (auto& f : featurizer.features) {
        f->clear();
        f->parse(sample);
        f->dump_debug_csv(std::cout);
    }
    for (auto& cf : featurizer.cross_features) {
        cf->clear();
        for (auto* c : cf->constituents) {
            c->clear();
            c->parse(sample);
        }
        cf->dump_debug_csv(std::cout);
    }
    return 0;
}
