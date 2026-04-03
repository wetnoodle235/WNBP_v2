# Sport-by-Sport Vendor Checklist: Raw vs Normalized Curated

Status legend: `[x] covered`, `[ ] partial`, `[ ] review`, `[ ] ignored`.

## Sport Summary

| Sport | Raw vendor-sport pairs | Normalized files | Curated files | Curated categories | Covered pairs | Ignored pairs |
|---|---:|---:|---:|---:|---:|---:|
| dota2 | 2 | 0 | 0 | 0 | 0 | 2 |
| nba | 4 | 0 | 461 | 6 | 4 | 0 |
| ncaab | 1 | 0 | 4 | 4 | 0 | 1 |
| ncaaf | 2 | 0 | 441 | 61 | 1 | 1 |

## Season Year And Type Coverage

| Sport | Raw years | Normalized years | Curated years | Raw-only years (not curated) | Raw season types | Normalized season types | Curated season types | Type gaps (raw not curated) |
|---|---|---|---|---|---|---|---|---|
| dota2 | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | - | - | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | regular | - | - | regular |
| nba | 2000..2027 (10 yrs) | - | 2025, 2026 | 2000, 2019, 2020, 2021, 2022, 2023, 2024, 2027 | playoffs, postseason, preseason, regular | - | regular | playoffs, postseason, preseason |
| ncaab | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | - | 2026 | 2020, 2021, 2022, 2023, 2024, 2025 | regular | - | - | regular |
| ncaaf | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | - | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | - | regular | - | group, postseason, regular | - |

## Ignored Raw Volume

| Sport | Vendor | Raw files | Reason |
|---|---|---:|---|
| dota2 | pandascore | 52593 | Raw exists but no normalized/curated path detected |
| dota2 | opendota | 11566 | Raw exists but no normalized/curated path detected |
| ncaab | ufcstats | 57 | Raw exists but no normalized/curated path detected |
| ncaaf | ufcstats | 47 | Raw exists but no normalized/curated path detected |

## Vendor Checklist

### dota2

| Status | Vendor | Raw files | Raw years | Raw season types | Normalized vendor hits | Curated vendor hits | Notes |
|---|---|---:|---|---|---:|---:|---|
| [ ] ignored | opendota | 11566 | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | regular | 0 | 0 | Raw exists but no normalized/curated path detected |
| [ ] ignored | pandascore | 52593 | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | - | 0 | 0 | Raw exists but no normalized/curated path detected |

### nba

| Status | Vendor | Raw files | Raw years | Raw season types | Normalized vendor hits | Curated vendor hits | Notes |
|---|---|---:|---|---|---:|---:|---|
| [x] covered | espn | 44721 | 2019..2027 (9 yrs) | postseason, preseason, regular | 0 | 3 | Vendor signal appears in curated provider/source columns |
| [x] covered | nbastats | 61425 | 2000, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | playoffs, regular | 0 | 146 | Vendor signal appears in curated provider/source columns |
| [x] covered | odds | 5 | 2026 | - | 0 | 156 | Vendor signal appears in curated provider/source columns |
| [x] covered | oddsapi | 12 | 2025, 2026 | - | 0 | 156 | Vendor signal appears in curated provider/source columns |

### ncaab

| Status | Vendor | Raw files | Raw years | Raw season types | Normalized vendor hits | Curated vendor hits | Notes |
|---|---|---:|---|---|---:|---:|---|
| [ ] ignored | ufcstats | 57 | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | regular | 0 | 0 | Raw exists but no normalized/curated path detected |

### ncaaf

| Status | Vendor | Raw files | Raw years | Raw season types | Normalized vendor hits | Curated vendor hits | Notes |
|---|---|---:|---|---|---:|---:|---|
| [ ] ignored | ufcstats | 47 | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | regular | 0 | 0 | Raw exists but no normalized/curated path detected |
| [x] covered | oddsapi | 1 | 2025 | - | 0 | 3 | Vendor signal appears in curated provider/source columns |

## Raw Files Not Mapped To A Sport

| Vendor | Unknown raw files |
|---|---:|
| cfbdata | 85686 |
| ergast | 750 |
| espn | 699714 |
| footballdata | 9754 |
| lahman | 12 |
| mlbstats | 23822 |
| nbastats | 168 |
| nhl | 48023 |
| odds | 90 |
| oddsapi | 101 |
| opendota | 0 |
| openf1 | 2869 |
| pandascore | 220006 |
| statsbomb | 698 |
| tennisabstract | 41 |
| ufcstats | 10847 |
| understat | 10974 |
| weather | 176 |