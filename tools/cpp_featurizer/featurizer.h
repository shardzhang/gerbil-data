#pragma once
#include <vector>
#include <memory>
#include "types.h"
#include "sample.h"
#include "feature.h"
#include "pos_map.h"

class ML1MFeaturizer {
public:
    PosMap      pos_map;
    TargetMap   target_map;
    int64_t     pos_map_timestamp = 0;

    std::vector<std::unique_ptr<AbstractFeature>> features;
    std::vector<std::unique_ptr<CrossFeature>>    cross_features;

    bool load_pos_map(const std::string& path) {
        pos_map.clear();
        target_map.clear();
        return ::load_pos_map(path, pos_map, target_map, pos_map_timestamp);
    }

    void register_feature(AbstractFeature* f) {
        features.emplace_back(f);
    }

    void register_cross(CrossFeature* cf) {
        cross_features.emplace_back(cf);
    }

    // Parse all features, encode with pos-map lookup, collect results
    std::vector<EncodedFeature> encode(const ML1MSample& sample) {
        std::vector<EncodedFeature> results;

        for (auto& f : features) {
            f->clear();
            f->parse(sample);
            EncodedFeature ef;
            ef.name = f->f_name;
            f->encode(pos_map, ef.values);
            results.push_back(std::move(ef));
        }

        for (auto& cf : cross_features) {
            cf->clear();
            // Parse all constituent features
            for (auto* c : cf->constituents) {
                c->clear();
                c->parse(sample);
            }
            EncodedFeature ef;
            ef.name = cf->f_name;
            ef.values = cf->encode(pos_map);
            results.push_back(std::move(ef));
        }

        return results;
    }

    // Parse and encode without pos-map (for building pos_map or raw hash debug)
    std::vector<EncodedFeature> encode_raw(const ML1MSample& sample, int64_t dim = MAX_DIM) {
        std::vector<EncodedFeature> results;
        for (auto& f : features) {
            f->clear();
            f->parse(sample);
            EncodedFeature ef;
            ef.name = f->f_name;
            f->encode(dim, ef.values);
            results.push_back(std::move(ef));
        }
        for (auto& cf : cross_features) {
            cf->clear();
            for (auto* c : cf->constituents) {
                c->clear();
                c->parse(sample);
            }
            EncodedFeature ef;
            ef.name = cf->f_name;
            ef.values = cf->encode(dim);
            results.push_back(std::move(ef));
        }
        return results;
    }
};
