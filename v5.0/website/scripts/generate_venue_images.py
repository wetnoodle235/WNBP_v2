#!/usr/bin/env python3
"""
Generate sportypy venue preview images for WNBP website.

Usage:
    pip install sportypy matplotlib
    python scripts/generate_venue_images.py

Outputs PNG files to public/venues/ (committed to the repo as static assets).
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
# Import from specific submodules to avoid name collision (NCAACourt exists in both basketball & tennis)
from sportypy.surfaces.basketball import NBACourt, WNBACourt, NCAACourt as NCAABCourt, FIBACourt
from sportypy.surfaces.football  import NFLField, NCAAField
from sportypy.surfaces.baseball  import MLBField
from sportypy.surfaces.hockey    import NHLRink
from sportypy.surfaces.soccer    import EPLPitch, MLSPitch
from sportypy.surfaces.tennis    import ATPCourt, WTACourt
import os

OUT = "/home/derek/Documents/stock/v5.0/website/public/venues"
DPI = 150

def save(ax, name, w, h, bg="#1a1a2e"):
    fig = ax.get_figure()
    fig.set_size_inches(w, h)
    fig.patch.set_facecolor(bg)
    fig.savefig(f"{OUT}/{name}.png", dpi=DPI, bbox_inches="tight",
                facecolor=bg, edgecolor="none")
    plt.close(fig)
    kb = os.path.getsize(f"{OUT}/{name}.png") // 1024
    print(f"  {name}.png  {kb} KB")

# ── Basketball ────────────────────────────────────────────────────────
print("Basketball...")
for cls, name in [(NBACourt,"nba_court"),(WNBACourt,"wnba_court"),(NCAABCourt,"ncaa_basketball_court")]:
    c = cls(color_updates={"plot_background":"#c68642"})
    save(c.draw(), name, 9, 5)

# ── Football ──────────────────────────────────────────────────────────
print("Football...")
for cls, name in [(NFLField,"nfl_field"),(NCAAField,"ncaa_football_field")]:
    f = cls(color_updates={"plot_background":"#2d7318"})
    save(f.draw(), name, 10, 5.5)

# ── Baseball ──────────────────────────────────────────────────────────
print("Baseball...")
b = MLBField(color_updates={"plot_background":"#3a8c3a"})
save(b.draw(), "mlb_field", 6.5, 6.5)

# ── Hockey ────────────────────────────────────────────────────────────
print("Hockey...")
r = NHLRink(color_updates={"plot_background":"#d8edf5"})
save(r.draw(), "nhl_rink", 10, 5)

# ── Soccer ────────────────────────────────────────────────────────────
print("Soccer...")
sc = EPLPitch(color_updates={
    "plot_background":"#2d6a2d","defensive_half_pitch":"#2d6a2d",
    "offensive_half_pitch":"#356935","pitch_apron":"#256025",
})
save(sc.draw(), "soccer_pitch", 9, 6)

# ── Tennis (use update_colors() — constructor kwargs don't propagate) ─
print("Tennis...")
SURFACES = {
    "hard":  {"plot_background":"#2858a0","ad_court":"#3468b8","deuce_court":"#3468b8",
               "backcourt":"#3468b8","doubles_alley":"#2858a0","court_apron":"#1a4080"},
    "clay":  {"plot_background":"#a04020","ad_court":"#b85230","deuce_court":"#b85230",
               "backcourt":"#b85230","doubles_alley":"#a04020","court_apron":"#8a3018"},
    "grass": {"plot_background":"#2d7a2d","ad_court":"#3a8c3a","deuce_court":"#3a8c3a",
               "backcourt":"#3a8c3a","doubles_alley":"#2d7a2d","court_apron":"#246024"},
}
for surface, colors in SURFACES.items():
    t = ATPCourt()
    t.update_colors(colors)
    ax = t.draw()
    save(ax, f"tennis_{surface}", 7, 4, colors["court_apron"])

print("Done!")


# ── CS2 Maps (awpy) ──────────────────────────────────────────────────
print("\nCS2 Maps (awpy)...")
CS2_MAPS = [
    "de_dust2", "de_mirage", "de_inferno", "de_nuke",
    "de_ancient", "de_anubis", "de_vertigo", "de_overpass",
    "de_train", "cs_italy",
]
try:
    from awpy.data.utils import create_data_dir_if_not_exists, fetch_resource
    from awpy.data import CURRENT_BUILD_ID, MAPS_DIR
    import awpy.plot as ap
    create_data_dir_if_not_exists()
    if not MAPS_DIR.exists():
        print("  Downloading CS2 map images...")
        fetch_resource('maps', CURRENT_BUILD_ID)
    for m in CS2_MAPS:
        try:
            fig, ax = ap.plot(m)
            fig.set_size_inches(6, 6)
            save(ax, f"cs2_{m}", 6, 6, "black")
        except Exception as e:
            print(f"  {m}: {e}")
except Exception as e:
    print(f"  Skipping CS2 maps: {e}")


# ── F1 Circuits (fastf1) ─────────────────────────────────────────────
print("\nF1 Circuits (fastf1)...")
F1_SESSIONS = [
    (2024, "Bahrain", "bahrain"),
]
try:
    import fastf1
    from scipy.ndimage import uniform_filter1d
    from matplotlib.collections import LineCollection
    fastf1.Cache.enable_cache('/tmp/f1cache')
    for year, event, key in F1_SESSIONS:
        try:
            session = fastf1.get_session(year, event, 'Q')
            session.load(laps=True, telemetry=True, weather=False, messages=False)
            fast_lap = session.laps.pick_fastest()
            tel = fast_lap.get_telemetry()
            x = uniform_filter1d(tel['X'].values, size=8)
            y = uniform_filter1d(tel['Y'].values, size=8)
            speed = tel['Speed'].values
            points = np.array([x, y]).T.reshape(-1, 1, 2)
            segments = np.concatenate([points[:-1], points[1:]], axis=1)
            lc = LineCollection(segments, cmap='RdYlGn', linewidth=4, alpha=0.95)
            lc.set_array(speed)
            fig, ax = plt.subplots(figsize=(8, 6))
            fig.patch.set_facecolor('#0d1117')
            ax.set_facecolor('#0d1117')
            ax.add_collection(lc)
            ax.autoscale_view()
            ax.set_aspect('equal')
            ax.axis('off')
            ax.plot(x[0], y[0], 'w^', ms=10, zorder=5)
            save(ax, f"f1_{key}", 8, 6, '#0d1117')
        except Exception as e:
            print(f"  {event}: {e}")
except Exception as e:
    print(f"  Skipping F1 circuits: {e}")
