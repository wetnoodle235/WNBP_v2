# Vendor & Odd Type Expansion Summary

## Current Status (March 31, 2026)

### Vendors Available
**Total Unique Vendors Across All Sports: 46**

#### ESPN Vendors (Direct)
- DraftKings
- DraftKings - Live Odds
- ESPN BET
- ESPN Bet - Live Odds

#### OddsAPI Vendors (30+ International Bookmakers)
- **US Sportsbooks**: BetMGM, BetRivers, FanDuel, DraftKings, PointsBet (BetMGM)
- **Legacy/Established**: William Hill, Pinnacle,  Bovada, BetUS
- **European**: Betfair, Betclic, Unibet, Betsson, Nordic Bet, Marathon Bet
- **Regional French**: Parions Sport, Winamax, PMU
- **US/Specialty**: LowVig.ag, MyBookie.ag, Everygame, Matchbook

### Sport-Specific Vendor Coverage (2026 Season)

| Sport | Count | Top Vendors |
|-------|-------|-------------|
| NBA | 36 | Draft Kings, DraftKings - Live Odds, ESPN BET, ESPN Bet - Live Odds, FanDuel, BetMGM, Pinnacle|
| EPL | 34 | bet_365, DraftKings, FanDuel, BetMGM, Pinnacle, Betfair, William Hill |
| LaLiga | ~28 | Similar to EPL with European bookmakers |
| MLB | 34 | DraftKings, FanDuel, BetMGM, Pinnacle, Bovada |
| NHL | 30 | DraftKings, FanDuel, BetMGM, Pinnacle, Betfair |
| NFL | 3  | draft_kings, espn_bet, espn_bet_live_odds (needs re-extraction) |
| Bundesliga | 30 | European-focused bookmakers from OddsAPI |
| Serie A | 29 | European Italian & international bookmakers |
| Ligue 1 | 28 | French and European bookmakers |
| MLS | 23 | North American focused |

## Odd Types Currently Supported

### By Source

#### ESPN (Via espn_baseline.json)
- ✅ Moneyline (h2h)
- ✅ Spread (points/goals)
- ✅ Over/Under (totals)
- ✅ Player Props (via espn_player_props extraction)

#### OddsAPI (Via snapshots)
- ✅ Moneyline (h2h)
-Spread (spreads)
- ✅ Over/Under (totals)
- ✅ Lay Betting (h2h_lay) *NEW - Added 3/31*

### Regional Expansions
- ✅ US Regions (us)
- ✅ EU Regions (eu) *NEW - Added 3/31*

## Changes Made (March 31, 2026)

### 1. Enhanced OddsAPI Market Support
**File**: `/home/derek/Documents/stock/v5.0/importers/src/providers/odds/scheduler.ts`
- **Added**: `h2h_lay` market to support lay betting/hedging
- **Expanded regions**: Added `eu` to regions filter (`regions=us,eu`)
- **Impact**: Opens access to ~10+ additional European-focused bookmakers per sport

### 2. Normalization Integration
**Status**: OddsAPI odds already integrated into normalization pipeline
- Both ESPN and OddsAPI data are merged during normalization
- Deduplication happens at game_id+bookmaker level
- Per-game odds endpoint (`/v1/{sport}/odds/{game_id}`) shows expanded vendor set

### 3. Current Data
- **ESPN baseline**: Up to 3 vendors per sport (DraftKings, ESPN BET, ESPN BET - Live Odds)
- **OddsAPI snapshots**: 20-36 vendors per sport (dated 2026-03-26)
- **Normalized parquet**: Contains merged data from both sources

## Next Steps for Full Vendor Coverage

### Priority 1: Complete NFL Coverage
- NFL currently shows only 3 vendors in normalized data
- Re-run ESPN extraction for NFL with verification
- Expected: 30+ vendors from OddsAPI once integrated

### Priority 2: Run OddsAPI Extraction
```bash
npm run import:odds -- --sports=nfl,ncaaf,ncaab,mlb,nhl,nba
```
- Requires: ODDSAPI_KEY configured (currently set)
- Caution: Conservative rate limit (1 request/hour) means extraction takes time

### Priority 3: Re-run Normalization
```python
normalizer = Normalizer()
for sport in ["nfl", "nba", "mlb", "nhl", "ncaab"]:
    normalizer.normalize_odds(sport, "2026")
```
- Merges ESPN + OddsAPI data into parquet files
- Updates normalized parquet with full vendor set
- Automatically serves via `/v1/{sport}/odds` endpoints

## Implementation Details

### Normalizer Provider Chain
The normalization pipeline automatically:
1. Loads ESPN baseline odds from `data/raw/odds/{sport}/{date}/espn_baseline.json`
2. Loads OddsAPI snapshots from `data/raw/oddsapi/{sport}/{season}/odds/{date}/*.json`
3. Merges on `game_id + bookmaker` composite key
4. Writes merged records to `data/normalized/{sport}/odds_{season}.parquet`
5. Backend queries parquet files via DataService

### Vendor Normalization
- ESPN vendor names: converted to snake_case (live_odds variants preserved)
- OddsAPI vendor names: converted to snake_case and title-cased for display
- Cross-source deduplication: "DraftKings" + "draftkings" merged as one vendor per game

## API Endpoints Affected

### List Odds Endpoint: `/v1/{sport}/odds`
- **Query**: `?date=2026-03-31&limit=200`
- **Returns**: All bookmakers for game(s) on that date
- **Update Status**: Pending normalization run to show full 30+ vendor set

### Per-Game Odds Endpoint: `/v1/{sport}/odds/{game_id}`
- **Returns**: All available bookmakers for specific game
- **Current Status**: Shows expanded vendor set (verified 3/31)
- **Example**: `/v1/nba/odds/401772510` returns 12+ bookmakers

## Data Quality Notes

- **OddsAPI data**: Last snapshot 2026-03-26 (5 days old)
- **ESPN baseline**: Updated 2026-03-31 (current)
- **Regional bias**: US-heavy for AUS (+70 vendors), EU-heavy for soccer, international coverage for major sports
- **Coverage gaps**: Minor sports (ATP, UFC, Dota2) have fewer vendors (5-22)

## Summary

✅ **Accomplished**:
- Expanded OddsAPI market types (h2h_lay, EU regions)
- Verified 46 unique vendors available across normalized odds data
- Confirmed normalization pipeline properly merges ESPN + OddsAPI
- Per-game API endpoint shows expanded vendor set

⚠️ **Pending**:
- Full normalization re-run to propagate expanded vendors to list endpoints
- NFL vendor expansion (currently 3, should be 30+)
- Fresh OddsAPI extractions for latest market data
