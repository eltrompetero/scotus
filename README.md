# Court Voting Data Module
Written by Eddie Lee.

A Python 3 package for loading court voting data. Each module focuses on a single court
system:

| Module          | System |
|-----------------|--------|
| `scotus.py`     | U.S. Supreme Court — modern Supreme Court Database (SCDB) vote tables (`ScotusData`) and the historical Warren-era conference vs. report votes (`ConferenceReportVotes`). |
| `high_courts.py`| International high courts — Canada, Australia, India (`HighCourt`). |
| `states.py`     | U.S. state supreme courts (`State`, `list_possible_states`). |

Shared internals: `_config.py` (data directory) and `_courts_common.py` (natural-court
extraction).

Modern SCOTUS data is from the Supreme Court Database Project
[http://supremecourtdatabase.org]. U.S. state data is from the State Supreme Court Data
Project [http://www.ruf.rice.edu/~pbrace/statecourt/].

## Requirements
`numpy`, `pandas`, `scipy` (see `requirements.txt`).

## Data directory
The package reads pickled and raw data from a single directory. By default this is
`~/Dropbox/Research/data_sets/scotus`; override it with the `SCOTUS_DATA_DIR` environment
variable:

```bash
export SCOTUS_DATA_DIR=/path/to/scotus/data
```

Expected files include the SCDB CSVs (`SCDB_<year>_01_justiceCentered_Citation.csv`),
`justices.csv`, the per-country `*_full_court_votes.p` pickles, the
`us_state_court_pickles/` directory, and the Warren-era `warren_conf_votes*.p` pickles.

## Usage

```python
from scotus import ScotusData, ConferenceReportVotes, HighCourt, State

# Modern SCOTUS
data = ScotusData()                 # latest cached SCDB year
votes = data.vote_table()           # justices x cases
data.second_rehnquist_court()       # 1994-2005 voting record

# Warren-era conference vs. report votes
warren = ConferenceReportVotes()
conf, report = warren.conference_and_report('PStewartWarren')

# International high courts
canada = HighCourt.get_court('canada')

# U.S. state supreme courts
md = State('MD').vote_table()
```

## Building the data
The `setup_*` functions rebuild the pickles from raw sources:

- `scotus.setup_warren_votes()` → `warren_conf_votes.p`
- `scotus.setup_warren_votes_by_court()` → `warren_conf_votes_bycourt.p`
  (and `..._natct.p`); run after `setup_warren_votes()`. Ported from the prototyping
  notebook; note that the data set's own natural-court labels misidentify some courts, so a
  heuristic drops justices who appear in fewer than 10% of a court's cases.
- `high_courts.setup_canada()` / `setup_australia()` / `setup_india()` →
  `*_full_court_votes.p`, built from `original_data_files/HCJD_*.dta`. Only Canada and
  Australia ship a prebuilt pickle; run `setup_india()` to build the India one.
- `states.setup_us_states()` → `us_state_court_pickles/`.

> Note: the Warren-era source pickles (`warren_conf_votes.p`, `vinwar_stata_EDL.xlsx.p`)
> were written with pandas <0.20 and only unpickle under a legacy pandas (e.g. ~1.0). Run the
> two `setup_warren_*` functions in such an environment; they emit a numpy-only
> `warren_conf_votes_bycourt.p` that `ConferenceReportVotes` then reads under any pandas
> version.
