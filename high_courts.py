# ====================================================================================== #
# Voting data for national high courts other than the U.S. (Canada, Australia, India).
# Provides access to the pickled court records and the setup routines that build them
# from the raw HCJD Stata files.
# Author: Eddie Lee, edlee@alumni.princeton.edu
# 2016-05-16
# ====================================================================================== #
import os
import pickle
import logging

import numpy as np
import pandas as pd

from ._config import DATADR
from ._courts_common import full_court_vote_sets

log = logging.getLogger(__name__)

# Pickle file and full-court size per supported country.
_COURT_FILES = {
    'canada': 'canada_full_court_votes.p',
    'australia': 'australian_full_court_votes.p',
    'india': 'india_full_court_votes.p',
}
_COURT_SIZES = {'canada': 9, 'australia': 7, 'india': 9}


def _resolve_country(name):
    """Map a free-form name to a canonical country key."""
    name = name.lower()
    if 'can' in name:
        return 'canada'
    if 'aus' in name:
        return 'australia'
    if 'ind' in name:
        return 'india'
    raise ValueError("Invalid court option: %r" % name)


class HighCourt():
    """Access to pickled high-court voting records for Canada, Australia, and India."""

    @classmethod
    def get_court(cls, name):
        """Return the list of natural-court vote sets for the named country."""
        country = _resolve_country(name)
        with open(os.path.join(DATADR, _COURT_FILES[country]), 'rb') as f:
            return pickle.load(f)['courts']

    @classmethod
    def full_court_size(cls, name):
        """Return the number of justices on a full court for the named country."""
        return _COURT_SIZES[_resolve_country(name)]

    @classmethod
    def save_court(cls, name, courts):
        """Overwrite the pickled vote records for the named country."""
        country = _resolve_country(name)
        path = os.path.join(DATADR, _COURT_FILES[country])
        if os.path.isfile(path):
            os.remove(path)
        with open(path, 'wb') as f:
            pickle.dump({'courts': courts}, f, -1)


# ====================================================================================== #
# Setup routines that build the pickled records from the raw HCJD Stata files.           #
# ====================================================================================== #
def setup_australia(min_votes=20):
    """Build australian_full_court_votes.p from HCJD_Australia.dta (7-justice courts)."""
    df = pd.read_stata(os.path.join(DATADR, 'original_data_files', 'HCJD_Australia.dta'),
                       convert_categoricals=False)

    # Vote columns contain 'v_'.
    df = df[np.sort([c for c in df.columns if 'v_' in c])]

    courts = full_court_vote_sets(df, court_size=7, min_votes=min_votes,
                                  justice_name_fn=lambda c: c.split('_')[1])
    log.info("Overwriting Australian court information...")
    HighCourt.save_court('australia', courts)


def setup_india(keepv2=True, min_votes=20):
    """Build india_full_court_votes.p from HCJD_India.dta (9-justice courts)."""
    df = _load_dual_issue('HCJD_India.dta', keepv2)
    courts = full_court_vote_sets(df, court_size=9, min_votes=min_votes,
                                  justice_name_fn=lambda c: c.split('_')[0])
    log.info("Overwriting Indian court information...")
    HighCourt.save_court('india', courts)


def setup_canada(keepv2=True, min_votes=20):
    """Build canada_full_court_votes.p from HCJD_Canada.dta (9-justice courts)."""
    df = _load_dual_issue('HCJD_Canada.dta', keepv2)
    courts = full_court_vote_sets(df, court_size=9, min_votes=min_votes,
                                  justice_name_fn=lambda c: c.split('_')[0])
    log.info("Overwriting Canadian court information...")
    HighCourt.save_court('canada', courts)


def _load_dual_issue(filename, keepv2):
    """Load an HCJD Stata file, keeping issue-1 votes and optionally stacking issue-2 votes."""
    df = pd.read_stata(os.path.join(DATADR, 'original_data_files', filename),
                       convert_categoricals=False)
    # Vote columns end in v1 / v2 (votes on issues 1 and 2).
    v1_cols = np.sort([c for c in df.columns if 'v1' in c])
    if keepv2:
        # Stack the second-issue votes underneath as additional cases.
        v2_cols = np.sort([c for c in df.columns if 'v2' in c])
        df1 = df[v1_cols]
        df2 = df[v2_cols]
        df2.columns = [c.replace('2', '1') for c in df2.columns]
        return pd.concat([df1, df2])
    return df[v1_cols]


if __name__ == '__main__':
    setup_canada(keepv2=False)
