"""Draw the published freshness comparison from curated, completed results.

Run from any directory after installing matplotlib. This reads only local data;
it does not connect to Snowflake. See docs/results.md for measurement scope.
"""
import csv
import json
import math
from pathlib import Path
import statistics

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]


def main():
    data = json.loads((ROOT/'docs/data/sustained-results.json').read_text())
    runs = data['paced_runs']
    if len(runs) != 3 or any(r['status'] != 'passed' or r['unobserved_panel_events'] for r in runs):
        raise ValueError('The figure requires all three completed runs and full panel coverage')
    with (ROOT/'docs/data/summary-visibility.csv').open() as handle:
        samples = list(csv.DictReader(handle))
    for run in runs:
        values = [float(r['latency_seconds']) for r in samples
                  if int(r['run']) == run['run'] and int(r['sequence']) > 120]
        if len(values) != run['post_warmup_delay_seconds']['n']:
            raise ValueError('CSV and result sample counts differ')
        for key, value in [('p50', statistics.median(values)),
                           ('p95', statistics.quantiles(values, n=100, method='inclusive')[94])]:
            if not math.isclose(value, run['post_warmup_delay_seconds'][key], rel_tol=0, abs_tol=1e-9):
                raise ValueError('CSV and result quantiles differ')
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 12,
                         'axes.spines.top': False, 'axes.spines.right': False})
    fig, ax = plt.subplots(figsize=(9, 4.8), layout='constrained')
    x = list(range(len(runs)))
    width = .34
    for shift, key, label, color in [
        (-width/2, 'p50', 'Half of readings included', '#086788'),
        (width/2, 'p95', '95% of readings included', '#dd7b27'),
    ]:
        bars = ax.bar([i+shift for i in x],
                      [r['post_warmup_delay_seconds'][key] for r in runs],
                      width, label=label, color=color)
        ax.bar_label(bars, fmt='%.1f s', padding=4, fontsize=11)
    ax.set_xticks(x, [f'Run {i+1}' for i in x])
    ax.set_ylabel('Seconds from send to observed correct summary')
    ax.set_ylim(0, max(r['post_warmup_delay_seconds']['p95'] for r in runs)*1.35)
    ax.set_title('How soon did the minute summary include a reading?',
                 loc='left', fontsize=16, weight='bold', pad=32)
    ax.text(0, 1.035, '1,000 sensors · one reading per second · three 30-minute runs',
            transform=ax.transAxes, fontsize=11)
    ax.legend(frameon=False, loc='upper left', fontsize=10)
    ax.grid(axis='y', alpha=.18)
    ax.set_axisbelow(True)
    fig.text(.08, -.045,
             'Four-sensor observation panel; first two minutes excluded. Five-second polling adds delay.\n'
             'All sensors and minute summaries were checked at the end. These results are not a service guarantee.',
             fontsize=9, color='#475569')
    for suffix in ['png', 'svg']:
        fig.savefig(ROOT/f'assets/summary-freshness.{suffix}', dpi=190, bbox_inches='tight')
    plt.close(fig)


if __name__ == '__main__':
    main()
