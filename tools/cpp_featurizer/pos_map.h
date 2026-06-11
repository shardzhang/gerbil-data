#pragma once
#include <fstream>
#include <string>
#include <cstdint>
#include "types.h"

inline std::string read_java_utf(std::ifstream& in) {
    uint16_t len_be;
    in.read(reinterpret_cast<char*>(&len_be), 2);
    uint16_t len = (len_be >> 8) | (len_be << 8);
    std::string s(len, '\0');
    if (len > 0) in.read(&s[0], len);
    return s;
}

template<typename T>
inline T read_le(std::ifstream& in) {
    T val;
    in.read(reinterpret_cast<char*>(&val), sizeof(T));
    return val;
}

inline bool load_pos_map(const std::string& path, PosMap& pos_map, TargetMap& target_map, int64_t& timestamp) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return false;

    timestamp = read_le<int64_t>(in);
    int32_t pos_map_size = read_le<int32_t>(in);

    for (int32_t i = 0; i < pos_map_size; i++) {
        PosMapEntry e;
        e.field_name  = read_java_utf(in);
        e.field_index = read_le<int32_t>(in);
        e.field_type  = read_le<int32_t>(in);
        e.dim         = read_le<int32_t>(in);
        e.hash        = read_le<int64_t>(in);
        e.pos         = read_le<int32_t>(in);
        e.mean        = read_le<double>(in);
        e.std         = read_le<double>(in);
        pos_map[e.field_index][e.hash] = e;
    }

    int32_t target_map_size = read_le<int32_t>(in);
    for (int32_t i = 0; i < target_map_size; i++) {
        int32_t raw_target = read_le<int32_t>(in);
        int32_t mapped_target = read_le<int32_t>(in);
        target_map[raw_target] = mapped_target;
    }

    in.close();
    return true;
}
