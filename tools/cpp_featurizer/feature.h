#pragma once
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <unordered_set>
#include <sstream>
#include <ostream>
#include "types.h"
#include "sample.h"
#include "MurmurHash3.h"

// ---------------------------------------------------------------------------
// MurmurHash3 helpers
// ---------------------------------------------------------------------------
inline uint64_t murmur_hash(const uint8_t* key, int len) {
    uint64_t out[2];
    MurmurHash3_x64_128(key, len, (uint32_t)SEED, out);
    return out[0];
}

inline uint64_t compute_hash(int32_t f_index, int64_t fea, int64_t dim) {
    uint8_t key[12];
    memcpy(key, &f_index, 4);
    memcpy(key + 4, &fea, 8);
    uint64_t out[2];
    MurmurHash3_x64_128(key, 12, (uint32_t)SEED, out);
    uint64_t hash = out[0] % (uint64_t)dim;
    return hash;
}

// ---------------------------------------------------------------------------
// AbstractFeature base
// ---------------------------------------------------------------------------
class AbstractFeature {
public:
    const int32_t  f_index;
    const std::string f_name;
    const FeatureType f_type;

    std::vector<std::string> raw_list;
    std::vector<int64_t>     feature_list;
    std::vector<float>       value_list;

    AbstractFeature(int32_t idx, std::string name, FeatureType t)
        : f_index(idx), f_name(std::move(name)), f_type(t) {}
    virtual ~AbstractFeature() = default;

    virtual void parse(const ML1MSample& sample) = 0;

    void clear() {
        raw_list.clear();
        feature_list.clear();
        value_list.clear();
    }

    // Encode without pos-map: cat features use hash, cont features use id directly
    void encode(int64_t dim, std::vector<FeatureValue>& out) const {
        out.clear();
        out.emplace_back("R:", 0L, 1.0f);
        for (size_t i = 0; i < feature_list.size(); i++) {
            int64_t fea = feature_list[i];
            if (fea == 0) continue;
            int64_t pos;
            if (f_type == FeatureType::Continuous) {
                pos = fea;  // 1L always
            } else {
                pos = (int64_t)compute_hash(f_index, fea, dim);
            }
            out.emplace_back(raw_list[i], pos, value_list[i]);
        }
    }

    // Encode with pos-map lookup
    void encode(const PosMap& pos_map, std::vector<FeatureValue>& out) const {
        out.clear();
        out.emplace_back("R:", 0L, 1.0f);
        auto feature_map_it = pos_map.find(f_index);
        if (feature_map_it == pos_map.end()) return;
        const auto& feature_map = feature_map_it->second;

        for (size_t i = 0; i < feature_list.size(); i++) {
            int64_t fea = feature_list[i];
            if (fea == 0) continue;
            int64_t lookup_key;
            if (f_type == FeatureType::Continuous) {
                lookup_key = fea;  // continuous uses id directly (1L)
            } else {
                lookup_key = (int64_t)compute_hash(f_index, fea, MAX_DIM);
            }
            auto it = feature_map.find(lookup_key);
            if (it != feature_map.end()) {
                out.emplace_back(raw_list[i], (int64_t)it->second.pos, value_list[i]);
            }
        }
    }

    // Debug dump: outputs CSV matching DumpGoldenData.scala format
    // Columns: feature_name, field_index, field_type, raw, feature_id, hash, value
    void dump_debug_csv(std::ostream& os, int64_t dim = MAX_DIM) const {
        for (size_t i = 0; i < feature_list.size(); i++) {
            int64_t fea = feature_list[i];
            if (fea == 0) continue;
            int64_t hash_val;
            if (f_type == FeatureType::Continuous) {
                hash_val = fea;  // 1L
            } else {
                hash_val = (int64_t)compute_hash(f_index, fea, dim);
            }
            // value: trim trailing zeros to match Scala's Float.toString
            float v = value_list[i];
            std::string val_str = std::to_string(v);
            auto dot = val_str.find('.');
            if (dot != std::string::npos) {
                auto last = val_str.find_last_not_of('0');
                if (last == dot) last--;  // keep at least ".0"
                val_str = val_str.substr(0, last + 1);
            }
            os << f_name << "," << f_index << ","
               << (f_type == FeatureType::Categorical ? 1 : 0) << ","
               << raw_list[i] << "," << fea << "," << hash_val << ","
               << val_str << "\n";
        }
    }
};

// ---------------------------------------------------------------------------
// CrossFeature
// ---------------------------------------------------------------------------
class CrossFeature {
public:
    const int32_t  f_index;
    const std::string f_name;
    std::vector<AbstractFeature*> constituents;

    CrossFeature(int32_t idx, std::string name)
        : f_index(idx), f_name(std::move(name)) {}

    void add_constituent(AbstractFeature* f) { constituents.push_back(f); }

    void clear() {
        for (auto* c : constituents) c->clear();
    }

    std::vector<FeatureValue> encode(int64_t dim) const {
        std::vector<FeatureValue> out;
        out.emplace_back("R:", 0L, 1.0f);
        if (constituents.empty()) return out;

        std::vector<size_t> indices(constituents.size(), 0);
        while (true) {
            bool skip = false;
            std::stringstream comb;
            for (size_t i = 0; i < constituents.size(); i++) {
                if (constituents[i]->feature_list.empty() ||
                    constituents[i]->feature_list[indices[i]] == 0) {
                    skip = true;
                    break;
                }
                if (i > 0) comb << "__xx__";
                comb << constituents[i]->f_index << ":" << constituents[i]->raw_list[indices[i]];
            }
            if (!skip) {
                // Build hash key: [f_index_1:4][fea_1:8][f_index_2:4][fea_2:8]...
                size_t key_len = (4 + 8) * constituents.size();
                std::vector<uint8_t> key(key_len);
                size_t off = 0;
                for (size_t i = 0; i < constituents.size(); i++) {
                    memcpy(key.data() + off, &constituents[i]->f_index, 4); off += 4;
                    int64_t fea = constituents[i]->feature_list[indices[i]];
                    memcpy(key.data() + off, &fea, 8); off += 8;
                }
                uint64_t hash = murmur_hash(key.data(), (int)key_len);
                uint64_t pos = hash % (uint64_t)dim;
                out.emplace_back(comb.str(), (int64_t)pos, 1.0f);
            }

            // Advance indices (Cartesian product)
            size_t i = constituents.size();
            while (i > 0) {
                i--;
                if (indices[i] + 1 < constituents[i]->feature_list.size()) {
                    indices[i]++;
                    break;
                } else {
                    indices[i] = 0;
                    if (i == 0) return out;
                }
            }
        }
    }

    std::vector<FeatureValue> encode(const PosMap& pos_map) const {
        std::vector<FeatureValue> out;
        out.emplace_back("R:", 0L, 1.0f);
        if (constituents.empty()) return out;

        auto feature_map_it = pos_map.find(f_index);
        if (feature_map_it == pos_map.end()) return out;
        const auto& feature_map = feature_map_it->second;

        std::vector<size_t> indices(constituents.size(), 0);
        while (true) {
            bool skip = false;
            std::stringstream comb;
            for (size_t i = 0; i < constituents.size(); i++) {
                if (constituents[i]->feature_list.empty() ||
                    constituents[i]->feature_list[indices[i]] == 0) {
                    skip = true;
                    break;
                }
                if (i > 0) comb << "__xx__";
                comb << constituents[i]->f_index << ":" << constituents[i]->raw_list[indices[i]];
            }
            if (!skip) {
                size_t key_len = (4 + 8) * constituents.size();
                std::vector<uint8_t> key(key_len);
                size_t off = 0;
                for (size_t i = 0; i < constituents.size(); i++) {
                    memcpy(key.data() + off, &constituents[i]->f_index, 4); off += 4;
                    int64_t fea = constituents[i]->feature_list[indices[i]];
                    memcpy(key.data() + off, &fea, 8); off += 8;
                }
                uint64_t hash = murmur_hash(key.data(), (int)key_len);
                auto it = feature_map.find((int64_t)hash);
                if (it != feature_map.end()) {
                    out.emplace_back(comb.str(), (int64_t)it->second.pos, 1.0f);
                }
            }

            size_t i = constituents.size();
            while (i > 0) {
                i--;
                if (indices[i] + 1 < constituents[i]->feature_list.size()) {
                    indices[i]++;
                    break;
                } else {
                    indices[i] = 0;
                    if (i == 0) return out;
                }
            }
        }
    }

    void dump_debug_csv(std::ostream& os, int64_t dim = MAX_DIM) const {
        if (constituents.empty()) return;
        std::vector<size_t> indices(constituents.size(), 0);
        while (true) {
            bool skip = false;
            for (size_t i = 0; i < constituents.size(); i++) {
                if (constituents[i]->feature_list.empty() ||
                    constituents[i]->feature_list[indices[i]] == 0) {
                    skip = true;
                    break;
                }
            }
            if (!skip) {
                size_t key_len = (4 + 8) * constituents.size();
                std::vector<uint8_t> key(key_len);
                size_t off = 0;
                for (size_t i = 0; i < constituents.size(); i++) {
                    memcpy(key.data() + off, &constituents[i]->f_index, 4); off += 4;
                    int64_t fea = constituents[i]->feature_list[indices[i]];
                    memcpy(key.data() + off, &fea, 8); off += 8;
                }
                uint64_t raw_hash = murmur_hash(key.data(), (int)key_len);
                uint64_t hash_val = raw_hash % (uint64_t)dim;

                std::stringstream comb;
                for (size_t i = 0; i < constituents.size(); i++) {
                    if (i > 0) comb << "__xx__";
                    comb << constituents[i]->f_index << ":" << constituents[i]->raw_list[indices[i]];
                }
                os << f_name << "," << f_index << ",1,"
                   << comb.str() << "," << (int64_t)raw_hash << "," << (int64_t)hash_val << ",1.0\n";
            }
            size_t i = constituents.size();
            while (i > 0) {
                i--;
                if (indices[i] + 1 < constituents[i]->feature_list.size()) {
                    indices[i]++;
                    break;
                } else {
                    indices[i] = 0;
                    if (i == 0) return;
                }
            }
        }
    }
};

// =========================================================================
// All 58 concrete feature implementations
// =========================================================================

// ---- User Demographics ----

class UserAge : public AbstractFeature {
public:
    UserAge(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int32_t age = 0;
        try { age = std::stoi(sample.age); } catch (...) {}
        raw_list.push_back(sample.age);
        feature_list.push_back(age);
        value_list.push_back(1.0f);
    }
};

class UserGender : public AbstractFeature {
public:
    UserGender(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int64_t buck = 0;
        if (sample.gender == "M") buck = 1;
        else if (sample.gender == "F") buck = 2;
        raw_list.push_back(sample.gender);
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class UserOccupation : public AbstractFeature {
public:
    UserOccupation(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int32_t occ = 0;
        try { occ = std::stoi(sample.occupation); } catch (...) {}
        raw_list.push_back(sample.occupation);
        feature_list.push_back(occ);
        value_list.push_back(1.0f);
    }
};

// ---- User Rate Std (bucketed) ----

static int64_t bucket_rate_std(float std) {
    if (std <= 0.0f) return 1;
    if (std <= 1.0f) return 2;
    if (std <= 2.0f) return 3;
    return 4;
}

class UserRateStd : public AbstractFeature {
public:
    UserRateStd(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        float std = sample.user_rate_std;
        int64_t buck = bucket_rate_std(std);
        raw_list.push_back(std::to_string(std));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class UserRateStd7Day : public AbstractFeature {
public:
    UserRateStd7Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        float std = sample.user_rate_std_7day;
        int64_t buck = bucket_rate_std(std);
        raw_list.push_back(std::to_string(std));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class UserRateStd15Day : public AbstractFeature {
public:
    UserRateStd15Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        float std = sample.user_rate_std_15day;
        int64_t buck = bucket_rate_std(std);
        raw_list.push_back(std::to_string(std));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class UserRateStd30Day : public AbstractFeature {
public:
    UserRateStd30Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        float std = sample.user_rate_std_30day;
        int64_t buck = bucket_rate_std(std);
        raw_list.push_back(std::to_string(std));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

// ---- User Rate Std (continuous) ----

class UserRateStdContinue : public AbstractFeature {
public:
    UserRateStdContinue(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Continuous) {}
    void parse(const ML1MSample& sample) override {
        float val = sample.user_rate_std;
        raw_list.push_back(std::to_string(val));
        feature_list.push_back(1L);
        value_list.push_back(val);
    }
};

class UserRateStd7DayContinue : public AbstractFeature {
public:
    UserRateStd7DayContinue(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Continuous) {}
    void parse(const ML1MSample& sample) override {
        float val = sample.user_rate_std_7day;
        raw_list.push_back(std::to_string(val));
        feature_list.push_back(1L);
        value_list.push_back(val);
    }
};

class UserRateStd15DayContinue : public AbstractFeature {
public:
    UserRateStd15DayContinue(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Continuous) {}
    void parse(const ML1MSample& sample) override {
        float val = sample.user_rate_std_15day;
        raw_list.push_back(std::to_string(val));
        feature_list.push_back(1L);
        value_list.push_back(val);
    }
};

class UserRateStd30DayContinue : public AbstractFeature {
public:
    UserRateStd30DayContinue(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Continuous) {}
    void parse(const ML1MSample& sample) override {
        float val = sample.user_rate_std_30day;
        raw_list.push_back(std::to_string(val));
        feature_list.push_back(1L);
        value_list.push_back(val);
    }
};

// ---- User Movie Rate Count (bucketed) ----

static int64_t bucket_cnt_10(int32_t cnt) {
    if (cnt <= 10) return 1;
    if (cnt <= 30) return 2;
    if (cnt <= 50) return 3;
    if (cnt <= 100) return 4;
    return 5;
}

static int64_t bucket_cnt_0(int32_t cnt) {
    if (cnt == 0) return 1;
    if (cnt <= 10) return 2;
    if (cnt <= 30) return 3;
    if (cnt <= 50) return 4;
    if (cnt <= 100) return 5;
    return 6;
}

class UserMovieRateCnt : public AbstractFeature {
public:
    UserMovieRateCnt(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int64_t buck = bucket_cnt_10((int32_t)sample.user_rate_cnt);
        raw_list.push_back(std::to_string(sample.user_rate_cnt));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class UserMovieRateCnt7Day : public AbstractFeature {
public:
    UserMovieRateCnt7Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int64_t buck = bucket_cnt_10((int32_t)sample.user_rate_7day_cnt);
        raw_list.push_back(std::to_string(sample.user_rate_7day_cnt));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class UserMovieRateCnt15Day : public AbstractFeature {
public:
    UserMovieRateCnt15Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int64_t buck = bucket_cnt_0((int32_t)sample.user_rate_15day_cnt);
        raw_list.push_back(std::to_string(sample.user_rate_15day_cnt));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class UserMovieRateCnt30Day : public AbstractFeature {
public:
    UserMovieRateCnt30Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int64_t buck = bucket_cnt_0((int32_t)sample.user_rate_30day_cnt);
        raw_list.push_back(std::to_string(sample.user_rate_30day_cnt));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

// ---- User Avg Rate (bucketed) ----

static int64_t bucket_avg_rate(float avg) {
    if (avg == 0.0f) return 1;
    if (avg < 3.0f) return 2;
    if (avg < 4.0f) return 3;
    return 4;
}

class UserAvgRate : public AbstractFeature {
public:
    UserAvgRate(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        float avg = sample.user_avg_rate;
        int64_t buck = bucket_avg_rate(avg);
        raw_list.push_back(std::to_string(avg));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class UserAvgRate7Day : public AbstractFeature {
public:
    UserAvgRate7Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        float avg = sample.user_avg_rate_7day;
        int64_t buck = bucket_avg_rate(avg);
        raw_list.push_back(std::to_string(avg));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class UserAvgRate15Day : public AbstractFeature {
public:
    UserAvgRate15Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        float avg = sample.user_avg_rate_15day;
        int64_t buck = bucket_avg_rate(avg);
        raw_list.push_back(std::to_string(avg));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class UserAvgRate30Day : public AbstractFeature {
public:
    UserAvgRate30Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        float avg = sample.user_avg_rate_30day;
        int64_t buck = bucket_avg_rate(avg);
        raw_list.push_back(std::to_string(avg));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

// ---- User Avg Rate (continuous) ----

class UserAvgRateContinue : public AbstractFeature {
public:
    UserAvgRateContinue(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Continuous) {}
    void parse(const ML1MSample& sample) override {
        float val = sample.user_avg_rate;
        raw_list.push_back(std::to_string(val));
        feature_list.push_back(1L);
        value_list.push_back(val);
    }
};

class UserAvgRate7DayContinue : public AbstractFeature {
public:
    UserAvgRate7DayContinue(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Continuous) {}
    void parse(const ML1MSample& sample) override {
        float val = sample.user_avg_rate_7day;
        raw_list.push_back(std::to_string(val));
        feature_list.push_back(1L);
        value_list.push_back(val);
    }
};

class UserAvgRate15DayContinue : public AbstractFeature {
public:
    UserAvgRate15DayContinue(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Continuous) {}
    void parse(const ML1MSample& sample) override {
        float val = sample.user_avg_rate_15day;
        raw_list.push_back(std::to_string(val));
        feature_list.push_back(1L);
        value_list.push_back(val);
    }
};

class UserAvgRate30DayContinue : public AbstractFeature {
public:
    UserAvgRate30DayContinue(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Continuous) {}
    void parse(const ML1MSample& sample) override {
        float val = sample.user_avg_rate_30day;
        raw_list.push_back(std::to_string(val));
        feature_list.push_back(1L);
        value_list.push_back(val);
    }
};

// ---- Item Features ----

// Strip trailing " (year)" matching Scala:
//   replaceAll("\\s*\\(\\d+\\)\\s*$", "")
static std::string strip_trailing_year(const std::string& title) {
    // walk backwards: \s* \) \d+ \( \s*  anchored at end
    auto pos = title.size();
    // \s*$
    while (pos > 0 && (title[pos - 1] == ' ' || title[pos - 1] == '\t'))
        --pos;
    // \)$
    if (pos == 0 || title[pos - 1] != ')') return title;
    --pos;
    // \d+ (backwards)
    auto end_digits = pos;
    while (pos > 0 && title[pos - 1] >= '0' && title[pos - 1] <= '9')
        --pos;
    if (end_digits == pos) return title;  // no digits
    // \($
    if (pos == 0 || title[pos - 1] != '(') return title;
    --pos;
    // \s*$
    while (pos > 0 && (title[pos - 1] == ' ' || title[pos - 1] == '\t'))
        --pos;
    return title.substr(0, pos);
}

class MovieTitle : public AbstractFeature {
public:
    MovieTitle(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        std::string title = strip_trailing_year(sample.movie_title);
        title = trim(title);
        auto words = split_string(title, ' ');
        for (auto& w : words) {
            if (w.empty()) continue;
            uint64_t hash = murmur_hash((const uint8_t*)w.data(), (int)w.size());
            raw_list.push_back(w);
            feature_list.push_back((int64_t)hash);
            value_list.push_back(1.0f);
        }
    }
};

class MovieGenres : public AbstractFeature {
public:
    MovieGenres(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        for (auto& g : sample.movie_genres) {
            if (g.empty()) continue;
            uint64_t hash = murmur_hash((const uint8_t*)g.data(), (int)g.size());
            raw_list.push_back(g);
            feature_list.push_back((int64_t)hash);
            value_list.push_back(1.0f);
        }
    }
};

class MovieRateCount : public AbstractFeature {
public:
    MovieRateCount(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int64_t buck;
        int64_t cnt = sample.movie_rate_count;
        if (cnt == 0) buck = 1;
        else if (cnt <= 2) buck = 2;
        else if (cnt <= 5) buck = 3;
        else if (cnt <= 10) buck = 4;
        else if (cnt <= 20) buck = 5;
        else if (cnt <= 50) buck = 6;
        else if (cnt <= 100) buck = 7;
        else if (cnt <= 200) buck = 8;
        else if (cnt <= 500) buck = 9;
        else if (cnt <= 1000) buck = 10;
        else buck = 11;
        raw_list.push_back(std::to_string(cnt));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class MovieAvgRate : public AbstractFeature {
public:
    MovieAvgRate(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        double avg = sample.movie_avg_rate;
        int64_t buck;
        if (avg <= 0.0) buck = 1;
        else if (avg <= 1.0) buck = 2;
        else if (avg <= 2.0) buck = 3;
        else if (avg <= 2.5) buck = 4;
        else if (avg <= 3.0) buck = 5;
        else if (avg <= 3.5) buck = 6;
        else if (avg <= 4.0) buck = 7;
        else if (avg <= 4.5) buck = 8;
        else buck = 9;
        raw_list.push_back(std::to_string(avg));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class MovieGenreCnt : public AbstractFeature {
public:
    MovieGenreCnt(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int32_t cnt = (int32_t)sample.movie_genres.size();
        int64_t buck = cnt >= 3 ? 3 : cnt;
        raw_list.push_back(std::to_string(cnt));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class MovieHotRank : public AbstractFeature {
public:
    MovieHotRank(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int32_t rank = sample.movie_hot_rank;
        int64_t buck;
        if (rank <= 100) buck = 4;
        else if (rank <= 500) buck = 3;
        else if (rank <= 2000) buck = 2;
        else buck = 1;
        raw_list.push_back(std::to_string(rank));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class MoviePublishYear : public AbstractFeature {
public:
    MoviePublishYear(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int32_t year = sample.movie_publish_year;
        int64_t buck;
        if (year == 0) buck = 1;
        else if (year < 1970) buck = 2;
        else if (year < 1980) buck = 3;
        else if (year < 1990) buck = 4;
        else if (year < 2000) buck = 5;
        else if (year < 2010) buck = 6;
        else buck = 7;
        raw_list.push_back(std::to_string(year));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class MovieAvgRateContinue : public AbstractFeature {
public:
    MovieAvgRateContinue(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Continuous) {}
    void parse(const ML1MSample& sample) override {
        float val = (float)sample.movie_avg_rate;
        raw_list.push_back(std::to_string(val));
        feature_list.push_back(1L);
        value_list.push_back(val);
    }
};

// ---- Context Features ----

class ContextTimeHour : public AbstractFeature {
public:
    ContextTimeHour(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int64_t val = sample.time_hour + 1;
        raw_list.push_back(std::to_string(sample.time_hour));
        feature_list.push_back(val);
        value_list.push_back(1.0f);
    }
};

class ContextTimeArea : public AbstractFeature {
public:
    ContextTimeArea(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int64_t val = sample.time_area + 1;
        raw_list.push_back(std::to_string(sample.time_area));
        feature_list.push_back(val);
        value_list.push_back(1.0f);
    }
};

class ContextTimeWeek : public AbstractFeature {
public:
    ContextTimeWeek(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int64_t val = sample.week_day;
        raw_list.push_back(std::to_string(sample.week_day));
        feature_list.push_back(val);
        value_list.push_back(1.0f);
    }
};

class IsWeekend : public AbstractFeature {
public:
    IsWeekend(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        int64_t val = (sample.week_day == 6 || sample.week_day == 7) ? 2 : 1;
        raw_list.push_back(std::to_string(val));
        feature_list.push_back(val);
        value_list.push_back(1.0f);
    }
};

// ---- User Movie Rate Sequences ----

class UserMovieRate : public AbstractFeature {
public:
    UserMovieRate(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        size_t n = std::min((size_t)200, sample.user_movie_rates.size());
        for (size_t i = 0; i < n; i++) {
            int32_t movie_id = sample.user_movie_rates[i].first;
            int32_t rating   = sample.user_movie_rates[i].second;
            raw_list.push_back(std::to_string(movie_id));
            feature_list.push_back(movie_id);
            value_list.push_back((float)rating);
        }
    }
};

class UserMovieRate1Day : public AbstractFeature {
public:
    UserMovieRate1Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        size_t n = std::min((size_t)200, sample.user_movie_rate_1days.size());
        for (size_t i = 0; i < n; i++) {
            raw_list.push_back(std::to_string(sample.user_movie_rate_1days[i].first));
            feature_list.push_back(sample.user_movie_rate_1days[i].first);
            value_list.push_back((float)sample.user_movie_rate_1days[i].second);
        }
    }
};

class UserMovieRate3Day : public AbstractFeature {
public:
    UserMovieRate3Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        size_t n = std::min((size_t)200, sample.user_movie_rate_3days.size());
        for (size_t i = 0; i < n; i++) {
            raw_list.push_back(std::to_string(sample.user_movie_rate_3days[i].first));
            feature_list.push_back(sample.user_movie_rate_3days[i].first);
            value_list.push_back((float)sample.user_movie_rate_3days[i].second);
        }
    }
};

class UserMovieRate7Day : public AbstractFeature {
public:
    UserMovieRate7Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        size_t n = std::min((size_t)200, sample.user_movie_rate_7days.size());
        for (size_t i = 0; i < n; i++) {
            raw_list.push_back(std::to_string(sample.user_movie_rate_7days[i].first));
            feature_list.push_back(sample.user_movie_rate_7days[i].first);
            value_list.push_back((float)sample.user_movie_rate_7days[i].second);
        }
    }
};

class UserMovieRate15Day : public AbstractFeature {
public:
    UserMovieRate15Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        size_t n = std::min((size_t)200, sample.user_movie_rate_15days.size());
        for (size_t i = 0; i < n; i++) {
            raw_list.push_back(std::to_string(sample.user_movie_rate_15days[i].first));
            feature_list.push_back(sample.user_movie_rate_15days[i].first);
            value_list.push_back((float)sample.user_movie_rate_15days[i].second);
        }
    }
};

// ---- User Genre Rate Sequences (hashed genre name) ----

template<typename PairVec>
static void parse_genre_rate_vec(const PairVec& vec, std::vector<std::string>& raw,
                                  std::vector<int64_t>& fea, std::vector<float>& val) {
    size_t n = std::min((size_t)200, vec.size());
    for (size_t i = 0; i < n; i++) {
        const auto& g = vec[i].first;
        if (g.empty()) continue;
        uint64_t hash = murmur_hash((const uint8_t*)g.data(), (int)g.size());
        raw.push_back(g);
        fea.push_back((int64_t)hash);
        val.push_back((float)vec[i].second);
    }
}

class UserGenresRate : public AbstractFeature {
public:
    UserGenresRate(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        parse_genre_rate_vec(sample.user_genres_rates, raw_list, feature_list, value_list);
    }
};

class UserGenresRate1Day : public AbstractFeature {
public:
    UserGenresRate1Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        parse_genre_rate_vec(sample.user_genres_rate_1days, raw_list, feature_list, value_list);
    }
};

class UserGenresRate3Day : public AbstractFeature {
public:
    UserGenresRate3Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        parse_genre_rate_vec(sample.user_genres_rate_3days, raw_list, feature_list, value_list);
    }
};

class UserGenresRate7Day : public AbstractFeature {
public:
    UserGenresRate7Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        parse_genre_rate_vec(sample.user_genres_rate_7days, raw_list, feature_list, value_list);
    }
};

class UserGenresRate15Day : public AbstractFeature {
public:
    UserGenresRate15Day(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        parse_genre_rate_vec(sample.user_genres_rate_15days, raw_list, feature_list, value_list);
    }
};

// ---- User Genre Rate Counts ----

static void parse_genre_cnt_vec(const std::vector<std::pair<std::string, int32_t>>& vec,
                                 std::vector<std::string>& raw, std::vector<int64_t>& fea,
                                 std::vector<float>& val) {
    size_t n = std::min((size_t)200, vec.size());
    for (size_t i = 0; i < n; i++) {
        std::string gen = to_lower(trim(vec[i].first));
        if (gen.empty()) continue;
        uint64_t hash = murmur_hash((const uint8_t*)gen.data(), (int)gen.size());
        raw.push_back(gen);
        fea.push_back((int64_t)hash);
        val.push_back((float)vec[i].second);
    }
}

class UserGenresRateCnts : public AbstractFeature {
public:
    UserGenresRateCnts(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        parse_genre_cnt_vec(sample.user_genres_rate_cnts, raw_list, feature_list, value_list);
    }
};

class UserGenresRateCnt1Days : public AbstractFeature {
public:
    UserGenresRateCnt1Days(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        parse_genre_cnt_vec(sample.user_genres_rate_cnt_1days, raw_list, feature_list, value_list);
    }
};

class UserGenresRateCnt3Days : public AbstractFeature {
public:
    UserGenresRateCnt3Days(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        parse_genre_cnt_vec(sample.user_genres_rate_cnt_3days, raw_list, feature_list, value_list);
    }
};

class UserGenresRateCnt7Days : public AbstractFeature {
public:
    UserGenresRateCnt7Days(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        parse_genre_cnt_vec(sample.user_genres_rate_cnt_7days, raw_list, feature_list, value_list);
    }
};

class UserGenresRateCnt15Days : public AbstractFeature {
public:
    UserGenresRateCnt15Days(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        parse_genre_cnt_vec(sample.user_genres_rate_cnt_15days, raw_list, feature_list, value_list);
    }
};

// ---- User Top 3 Genres ----

class UserTop3Genres : public AbstractFeature {
public:
    UserTop3Genres(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        for (auto& tg : sample.user_top3_genres) {
            auto& g = tg.first;
            if (g.empty()) continue;
            uint64_t hash = murmur_hash((const uint8_t*)g.data(), (int)g.size());
            raw_list.push_back(g);
            feature_list.push_back((int64_t)hash);
            value_list.push_back((float)tg.second);
        }
    }
};

// ---- User Watch Same Genre (overlap check) ----

static std::unordered_set<std::string> to_set(const std::vector<std::pair<std::string, float>>& v) {
    std::unordered_set<std::string> s;
    for (auto& p : v) s.insert(p.first);
    return s;
}

class UserWatchSameGenre : public AbstractFeature {
public:
    UserWatchSameGenre(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        auto current = std::unordered_set<std::string>(sample.movie_genres.begin(), sample.movie_genres.end());
        auto recent  = to_set(sample.user_genres_rates);
        int64_t flag;
        if (current.empty() || recent.empty()) flag = 1;
        else {
            bool overlap = false;
            for (auto& g : current) {
                if (recent.count(g)) { overlap = true; break; }
            }
            flag = overlap ? 2 : 1;
        }
        raw_list.push_back(std::to_string(flag));
        feature_list.push_back(flag);
        value_list.push_back(1.0f);
    }
};

#define DEF_WATCH_SAME_GENRE(ClassName, Field) \
class ClassName : public AbstractFeature { \
public: \
    ClassName(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {} \
    void parse(const ML1MSample& sample) override { \
        auto current = std::unordered_set<std::string>(sample.movie_genres.begin(), sample.movie_genres.end()); \
        auto recent  = to_set(sample.Field); \
        bool overlap = false; \
        for (auto& g : current) { if (recent.count(g)) { overlap = true; break; } } \
        int64_t flag = overlap ? 2 : 1; \
        raw_list.push_back(std::to_string(flag)); \
        feature_list.push_back(flag); \
        value_list.push_back(1.0f); \
    } \
};

DEF_WATCH_SAME_GENRE(UserWatchSameGenre1Day,  user_genres_rate_1days)
DEF_WATCH_SAME_GENRE(UserWatchSameGenre3Day,  user_genres_rate_3days)
DEF_WATCH_SAME_GENRE(UserWatchSameGenre7Day,  user_genres_rate_7days)
DEF_WATCH_SAME_GENRE(UserWatchSameGenre15Day, user_genres_rate_15days)

// ---- User Same Genre Avg Rate (bucketed) ----

class UserSameGenreAvgRate : public AbstractFeature {
public:
    UserSameGenreAvgRate(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Categorical) {}
    void parse(const ML1MSample& sample) override {
        float final_rate = 3.0f;
        std::unordered_map<std::string, float> genre_map;
        for (auto& p : sample.user_genres_rates) genre_map[p.first] = p.second;
        float sum = 0.0f;
        int count = 0;
        for (auto& g : sample.movie_genres) {
            auto it = genre_map.find(g);
            if (it != genre_map.end()) {
                sum += it->second;
                count++;
            }
        }
        if (count > 0) final_rate = sum / count;
        int64_t buck;
        if (final_rate <= 1.0f) buck = 1;
        else if (final_rate <= 2.0f) buck = 2;
        else if (final_rate <= 3.0f) buck = 3;
        else if (final_rate <= 4.0f) buck = 4;
        else buck = 5;
        raw_list.push_back(std::to_string(final_rate));
        feature_list.push_back(buck);
        value_list.push_back(1.0f);
    }
};

class UserSameGenreAvgRateContinue : public AbstractFeature {
public:
    UserSameGenreAvgRateContinue(int32_t idx, std::string name) : AbstractFeature(idx, std::move(name), FeatureType::Continuous) {}
    void parse(const ML1MSample& sample) override {
        float final_rate = 3.0f;
        std::unordered_map<std::string, float> genre_map;
        for (auto& p : sample.user_genres_rates) genre_map[p.first] = p.second;
        float sum = 0.0f;
        int count = 0;
        for (auto& g : sample.movie_genres) {
            auto it = genre_map.find(g);
            if (it != genre_map.end()) {
                sum += it->second;
                count++;
            }
        }
        if (count > 0) final_rate = sum / count;
        raw_list.push_back(std::to_string(final_rate));
        feature_list.push_back(1L);
        value_list.push_back(final_rate);
    }
};
