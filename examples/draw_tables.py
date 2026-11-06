"""Render the article's comparison tables for editors without native tables.

Run with matplotlib installed. Values and assumptions match docs/costs.md.
These illustrations are budgets, not measured bills or capacity comparisons.
"""
from pathlib import Path
import textwrap

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

ROOT = Path(__file__).resolve().parents[1]


def draw(name, title, subtitle, headers, rows, widths, wraps, footer):
    """Draw a legible table; row heights grow with the wrapped cell content."""
    wrapped = [[textwrap.fill(cell, width) for cell, width in zip(row, wraps)]
               for row in rows]
    heights = [max(cell.count('\n') + 1 for cell in row) * 0.34 + 0.34
               for row in wrapped]
    top = 1.55
    bottom = 1.10
    total = top + 0.66 + sum(heights) + bottom
    fig, ax = plt.subplots(figsize=(16, total))
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.set(xlim=(0, 16), ylim=(total, 0))
    ax.axis('off')
    fig.patch.set_facecolor('#FFFFFF')
    ax.text(0.45, 0.48, title, fontsize=28, weight='bold', color='#12384A', va='center')
    ax.text(0.45, 1.02, subtitle, fontsize=17, color='#435564', va='center')
    y = top
    x = 0.4
    for header, width in zip(headers, widths):
        ax.add_patch(Rectangle((x, y), width, 0.66, color='#12384A'))
        ax.text(x + 0.18, y + 0.33, header, fontsize=18, weight='bold', color='white', va='center')
        x += width
    y += 0.66
    for i, (row, height) in enumerate(zip(wrapped, heights)):
        x = 0.4
        for j, (cell, width) in enumerate(zip(row, widths)):
            ax.add_patch(Rectangle((x, y), width, height,
                                   facecolor='#EDF5F8' if i % 2 == 0 else '#F8FAFB',
                                   edgecolor='white', linewidth=2))
            ax.text(x + 0.18, y + height / 2, cell, fontsize=18,
                    weight='bold' if j == 0 else 'normal', color='#173746',
                    va='center', linespacing=1.35)
            x += width
        y += height
    ax.text(0.45, y + 0.30, footer, fontsize=15, color='#435564', va='top', linespacing=1.45)
    for suffix in ('png', 'svg'):
        fig.savefig(ROOT / 'assets' / f'{name}.{suffix}', dpi=150,
                    facecolor=fig.get_facecolor())
    plt.close(fig)


def main():
    draw('cost-compute-table', 'Compute: a 30-day uptime budget',
         '720 active hours • Assumed price: US$3 per credit • One warehouse cluster / service baseline',
         ['Component', 'Credits / hour', 'Credits / month', 'US$ / month'],
         [
             ['Standard X-Small — Azure Gen2', '1.25', '900', '$2,700'],
             ['Interactive X-Small', '0.60', '432', '$1,296'],
             ['Online Feature Store XS', '0.76', '547.2', '$1,641.60'],
             ['Online Feature Store S', '2.66', '1,915.2', '$5,745.60'],
         ], [6.7, 2.7, 2.9, 2.9], [36, 15, 17, 16],
         'Different services and capacity; these are not equivalent-performance offers. Storage and other charges are additional.\nSource: Snowflake Service Consumption Table. Editable figures and formulas: docs/costs.md in the companion repository.')
    draw('cost-usage-table', 'Monthly charges for five example workloads',
         'Separate examples for 30 days • Assumed price: US$3 per credit • Each row covers only the named charge',
         ['Component', 'Explicit usage assumption', 'US$ / month'],
         [
             ['Raw streaming ingestion', '1,000 sensors × 1 reading/second; 100 billable bytes/reading', '$2.88'],
             ['Summary streaming ingestion', '1,000 sensors × 1 summary/minute; 200 billable bytes/summary', '$0.10'],
             ['Occasional SQL — Azure Gen2 XS', '12 billable warehouse minutes/day', '$22.50'],
             ['Serverless task compute', '1 task/minute, consuming 10 XS-equivalent compute-seconds each', '$324'],
             ['Materialized-view maintenance', '2 XS-equivalent compute-hours/day', '$360'],
         ], [5.3, 7.0, 2.9], [28, 42, 16],
         'XS-equivalent measures compute consumed, not elapsed job time. These are assumed workloads, not measurements.\nHigh-performance Snowpipe Streaming: 0.0037 credits/GB. Other compute, storage and processing are separate.')
    draw('storage-choices-table', 'What should you keep?',
         'Choose the history you need before choosing how to calculate the summary.',
         ['Storage choice', 'What you gain', 'What you give up / manage'],
         [
             ['Raw readings only', 'Full detail; freedom to ask new questions', 'Repeated calculation when queried'],
             ['Raw plus summaries', 'Detail for investigation; ready summaries', 'Extra refresh work and stored results'],
             ['Summaries only', 'Less incoming detail and smaller history', 'Individual spikes, exact order and later recalculation'],
             ['Recent raw plus retained summaries', 'Recent detail and longer summary history', 'Rules for closing and correcting old summaries'],
         ], [4.5, 5.2, 5.5], [24, 29, 31],
         'An ordinary Dynamic Table follows its source: deleting raw data can remove the summaries too.\nRetaining summaries requires a separate archive or an explicit frozen-history design.')
    draw('alternatives-table', 'Where should the calculation run?',
         'Choose by the answer your application needs and how fresh it must be.',
         ['Option', 'A useful reason to choose it'],
         [
             ['SQL query or view', 'Calculate statistics when someone asks.'],
             ['Dynamic Tables', 'Keep SQL summaries updated for dashboards and analysis.'],
             ['Streams and tasks', 'Control updates, corrections and retained summaries explicitly.'],
             ['Materialized view', 'Speed up an eligible repeated aggregation over one physical table.'],
             ['Streaming Transformations — private preview', 'Evaluate processing before rows land; requires access and validation of the exact window rules.'],
             ['Interactive warehouse', 'Serve SQL over raw or summary tables; maintaining stored summaries has separate refresh compute.'],
             ['Online Feature Store', 'Serve current per-sensor values through a lookup API, with supported streaming calculations.'],
         ], [5.5, 9.7], [30, 58],
         'Materialized views require Enterprise Edition and a supported query shape.\nThe trial verified the Dynamic Table and Interactive paths; Online provisioning was blocked.')


if __name__ == '__main__':
    main()
