# ====================================================================================== #
# U.S. state supreme court voting data (State Supreme Court Data Project).
# Provides access to the per-state pickled records plus the setup routine that splits the
# master file into per-state pickles.
# Author: Eddie Lee, edlee@alumni.princeton.edu
# ====================================================================================== #
import os
import logging

import numpy as np
import pandas as pd

from ._config import DATADR
from ._courts_common import extract_natural_courts

log = logging.getLogger(__name__)

# Maximum number of justice slots per case in the source schema (columns J1..J11).
N_JUSTICE_SLOTS = 11

# Vote codes used in the source data.
NO_DATA = -1
MINORITY = 0
MAJORITY = 1
RECUSED = 2
NOT_PARTICIPATING = 3

# Known name-coding errors in the source data, keyed by state then {wrong: right}.
_NAME_FIXES = {
    'ID': {'Jones': 'J. Jones'},
    'MD': {'J. Murrphy': 'J. Murphy'},
    'OH': {'Oconnor': 'OConnor'},
    'MI': {'294': 'Brickley', '581': 'Taylor'},
    'KS': {'Gernon ': 'Gernon'},
    'SC': {'Burrnett': 'Burnett'},
}


def list_possible_states():
    """Sorted list of state codes for which a pickle is available."""
    files = os.listdir(os.path.join(DATADR, 'us_state_court_pickles'))
    return sorted(f[:-2] for f in files)


class State():
    def __init__(self, state):
        """
        Parameters
        ----------
        state : str
            Two-letter state code.
        """
        self.state = state
        self.fname = os.path.join(DATADR, 'us_state_court_pickles', '%s.p' % state)
        if not os.path.isfile(self.fname):
            raise ValueError("Invalid state: %r" % state)

    def vote_table(self, clean=True, return_code=False, return_year=False):
        """Convert the default format into a table where rows are individual cases and each
        justice has a column. Many entries will be empty.

        Parameters
        ----------
        clean : bool, True
            If True, returned votes only include 0's and 1's; all other values become -1.
        return_code : bool, False
            If True, label columns by justice code instead of justice name.
        return_year : bool, False
            If True, also return a table of years per citation (access with X['year'].values).

        Returns
        -------
        pd.DataFrame
            Vote codes: -1 no data, 0 minority, 1 majority, 2 recused, 3 not participating.
        pd.DataFrame, optional
            Years, if return_year is True.
        """
        df = pd.read_pickle(self.fname)

        label_col, label_key = ('J%d_Code', 'code') if return_code else ('J%d_Name', 'name')
        subtables = []
        for i in range(1, N_JUSTICE_SLOTS + 1):
            cols = ('LexisNexisCitationNumber', 'Year', 'J%d_Vote' % i, label_col % i)
            sub = df.loc[:, cols].rename(columns={cols[0]: 'citation', cols[1]: 'year',
                                                  cols[2]: 'vote', cols[3]: label_key})
            subtables.append(sub)
        fullTable = pd.concat(subtables, axis=0)

        if not return_code:
            # Address some data-coding bugs in the justice names.
            for wrong, right in _NAME_FIXES.get(self.state, {}).items():
                fullTable.loc[(fullTable['name'] == wrong).values, 'name'] = right

        voteTable = pd.pivot_table(fullTable, columns=label_key, index='citation',
                                   fill_value=NO_DATA, dropna=False)['vote']
        if not return_code:
            # Throw out any blank justice names.
            voteTable = voteTable.loc[:, voteTable.columns != '']

        if not clean:
            return voteTable

        voteTable[((voteTable != MINORITY) & (voteTable != MAJORITY)).values] = NO_DATA
        if return_year:
            year = pd.pivot_table(fullTable, index='citation')
            assert len(year) == len(voteTable)
            return voteTable, year
        return voteTable

    @classmethod
    def extract_nat_courts(cls, X, only_full_votes=True, threshold_votes='default'):
        """Get indices for unique natural courts identified by unique subsets of voters.
        See _courts_common.extract_natural_courts for details.
        """
        return extract_natural_courts(X, only_full_votes=only_full_votes,
                                      threshold_votes=threshold_votes)

    def _vote_cols(self):
        """Source column names (vote, code, name) for every justice slot."""
        cols = ()
        for i in range(1, N_JUSTICE_SLOTS + 1):
            cols += ('J%d_Vote' % i, 'J%d_Code' % i, 'J%d_Name' % i)
        return cols


def setup_us_states():
    """Load the master file and split it by state into us_state_court_pickles/.

    State Supreme Court Data Project: http://www.ruf.rice.edu/~pbrace/statecourt/
    """
    cached = os.path.join(DATADR, 'state_supreme_court_v2.p')
    if os.path.isfile(cached):
        df = pd.read_pickle(cached)
    else:
        log.info("Unable to find pickled stat file. Loading from stata...")
        df = pd.read_stata(os.path.join(DATADR, 'state_supreme_court_v2.dta'))
        log.info("Caching to pickle...")
        df.to_pickle(cached)
    assert np.unique(df['state']).size == 52, "Expected 50 states + DC + national."

    for state in np.unique(df['state']):
        fname = os.path.join(DATADR, 'us_state_court_pickles', '%s.p' % state)
        log.info("Saving %s.", fname)
        df[(df['state'] == state).values].to_pickle(fname)
