#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <sstream>

static constexpr int64_t SEED = 0x3c074a61;
static constexpr int64_t MAX_DIM = 1LL << 60;

enum class FeatureType : int8_t {
    Continuous = 0,
    Categorical = 1
};

struct FeatureValue {
    std::string raw;
    int64_t     index;   // embedding position (after pos_map lookup)
    float       value;

    FeatureValue() : index(0), value(0.0f) {}
    FeatureValue(std::string r, int64_t i, float v) : raw(std::move(r)), index(i), value(v) {}
};

struct EncodedFeature {
    std::string              name;
    std::vector<FeatureValue> values;
};

struct PosMapEntry {
    std::string field_name;
    int32_t     field_index;
    int32_t     field_type;
    int32_t     dim;
    int64_t     hash;
    int32_t     pos;
    double      mean;
    double      std;
};

using PosMap = std::unordered_map<int64_t, std::unordered_map<int64_t, PosMapEntry>>;
using TargetMap = std::unordered_map<int32_t, int32_t>;

inline std::vector<std::string> split_string(const std::string& s, char delim) {
    std::vector<std::string> parts;
    std::istringstream ss(s);
    std::string part;
    while (std::getline(ss, part, delim)) {
        if (!part.empty()) parts.push_back(part);
    }
    return parts;
}

inline std::string trim(const std::string& s) {
    const char* ws = " \t\r\n";
    auto start = s.find_first_not_of(ws);
    if (start == std::string::npos) return "";
    auto end = s.find_last_not_of(ws);
    return s.substr(start, end - start + 1);
}

inline std::string to_lower(const std::string& s) {
    std::string r = s;
    for (auto& c : r) c = (char)tolower((unsigned char)c);
    return r;
}
