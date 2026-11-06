"""Draw the explanatory architecture figure, not an application screenshot.

Optional dependency: matplotlib. Run from any working directory.
"""
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

out = Path(__file__).resolve().parents[1] / 'assets'
out.mkdir(exist_ok=True)
fig, ax = plt.subplots(figsize=(15, 4.8))
fig.patch.set_facecolor('#f6f9fc')
ax.set(xlim=(0, 15), ylim=(0, 4.8))
ax.axis('off')
fig.subplots_adjust(left=.015, right=.985, bottom=.02, top=.98)
ink, blue, purple = '#17324d', '#007ea8', '#6d50a4'
ax.text(.2, 4.22, 'From sensor readings to a minute summary',
        fontsize=22, weight='bold', color=ink)
labels = [
    ('Sensors', 'One reading\nevery second', ink),
    ('Streaming pipe', 'Parse fields\nConvert values', blue),
    ('Raw readings', 'Store the messages\nthat arrived', blue),
    ('One copy per ID', 'Remove repeated\nmeasurements', purple),
    ('Minute summary', 'Count, average\nand standard deviation', purple),
]
for i, (title, detail, color) in enumerate(labels):
    x = .2 + i * 3
    ax.add_patch(FancyBboxPatch((x, 1.65), 2.55, 1.68,
        boxstyle='round,pad=0.02,rounding_size=.12',
        edgecolor=color, linewidth=1.8, facecolor='white'))
    ax.text(x+1.275, 2.9, title, ha='center', va='center', fontsize=13.2,
            color=color, weight='bold')
    ax.text(x+1.275, 2.23, detail, ha='center', va='center', fontsize=12,
            color=ink, linespacing=1.6)
    if i < 4:
        ax.add_patch(FancyArrowPatch((x+2.62, 2.5), (x+2.93, 2.5),
            arrowstyle='-|>', mutation_scale=16, color=ink, linewidth=1.5))
ax.text(4.48, 1.2, 'Pay for incoming data volume', ha='center', color=blue, fontsize=12)
ax.text(10.48, 1.2, 'Dynamic Tables use warehouse compute', ha='center', color=purple, fontsize=12)
ax.text(.2, .48, 'This recipe keeps raw readings and summaries. Their freshness and retention are separate choices.',
        fontsize=12.5, color=ink)
for extension in ['png', 'svg']:
    fig.savefig(out / f'main-flow.{extension}', dpi=180)
plt.close(fig)
