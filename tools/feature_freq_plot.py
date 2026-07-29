#!/usr/bin/env python3
"""
Read pos_map.json, compute feature frequency distribution, save CSV, and plot.
"""

import json
import sys
import csv
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np

def main(input_path, csv_path, png_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        root = json.load(f)

    features = root.get('features', [])
    cnts = []
    for feat in features:
        entries = feat.get('entries', [])
        for e in entries:
            cnts.append(e['count'])

    total = len(cnts)
    freq_map = Counter(cnts)
    sorted_items = sorted(freq_map.items())  # (count, num_features)

    cumul = 0
    rows = []
    for cnt, num in sorted_items:
        cumul += num
        pct = num / total * 100
        cumul_pct = cumul / total * 100
        rows.append((cnt, num, pct, cumul_pct))

    # Save CSV
    with open(csv_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['count', 'num_features', 'pct', 'cumul_pct'])
        w.writerows(rows)

    print(f"Saved CSV: {csv_path}")
    print(f"Total entries: {total}")
    print(f"Unique count values: {len(rows)}")
    print(f"Min count: {min(cnts)}, Max count: {max(cnts)}, Mean: {np.mean(cnts):.1f}, Median: {np.median(cnts):.0f}")
    print(f"Features appearing exactly once: {freq_map.get(1, 0)} ({freq_map.get(1, 0)/total*100:.1f}%)")

    # Plot
    x = [r[0] for r in rows]
    pct_y = [r[2] for r in rows]
    cumul_y = [r[3] for r in rows]

    fig, ax1 = plt.subplots(figsize=(14, 6))

    ax1.bar(x, pct_y, width=0.8, color='steelblue', alpha=0.7, label='% of features')
    ax1.set_xlabel('Feature frequency (count)')
    ax1.set_ylabel('% of features', color='steelblue')
    ax1.tick_params(axis='y', labelcolor='steelblue')
    ax1.set_xscale('log')
    ax1.set_xlim(left=0.5)

    ax2 = ax1.twinx()
    ax2.plot(x, cumul_y, color='crimson', marker='.', linestyle='-', linewidth=1, markersize=3, label='Cumulative %')
    ax2.set_ylabel('Cumulative %', color='crimson')
    ax2.tick_params(axis='y', labelcolor='crimson')
    ax2.set_ylim(0, 105)

    # Threshold lines
    for thresh in [50, 80, 90, 95]:
        ax2.axhline(y=thresh, color='gray', linestyle='--', linewidth=0.5, alpha=0.5)
        ax2.text(x[-1]*0.8, thresh + 1, f'{thresh}%', color='gray', fontsize=8)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='lower right')

    plt.title(f'Feature Frequency Distribution ({total:,} entries)')
    fig.tight_layout()
    plt.savefig(png_path, dpi=150)
    plt.close()
    print(f"Saved plot: {png_path}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 feature_freq_plot.py <pos_map.json> [output.csv] [output.png]")
        sys.exit(1)
    input_path = sys.argv[1]
    csv_path = sys.argv[2] if len(sys.argv) > 2 else 'feature_freq.csv'
    png_path = sys.argv[3] if len(sys.argv) > 3 else 'feature_freq_plot.png'
    main(input_path, csv_path, png_path)
