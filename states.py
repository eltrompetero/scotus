# Wrapper for loading US state Supreme Court data.
# Author: Eddie Lee, edlee@alumni.princeton.edu
import pandas as pd
import numpy as np
import os
DATADR = os.path.expanduser('~')+'/Dropbox/Research/py_lib/data_sets/scotus/'


def show_possible_states():
    files = os.listdir('%s/us_state_court_pickles'%DATADR)
    print(sorted(files))

class States():
    def __init__(self, state):
        """
        Parameters
        ----------
        state : str
        """
        self.state = state
        self.fname = '%s/us_state_court_pickles/%s.p'%(DATADR,self.state)
        if not os.path.isfile(self.fname):
            raise Exception("Invalid state.")

    def vote_table(self):
        """Convert default format into a table where rows are individual cases and each
        justice has a column.  Many entries will be empty.

        Returns
        -------
        pd.DataFrame
            -1, No data
             0, Minority
             1, Majority
             2, Recused
             3, Not participating
        """
        
        df = pd.read_pickle(self.fname)
        
        subTables = []
        for i in range(1,12):
            cols = ('LexisNexisCitationNumber',)+('J%d_Vote'%i, 'J%d_Name'%i)
            subTables.append( df.loc[:,cols] )
            subTables[-1].rename(columns={cols[0]:'citation', cols[1]:'vote', cols[2]:'name'},
                                 inplace=True)
        voteTable = pd.concat(subTables, axis=0)

        # address some coding bugs
        if self.state=='MD':
            voteTable.loc[(voteTable['name']=='J. Murrphy').values,'name'] = 'J. Murphy'

        voteTable = pd.pivot_table( voteTable,
                                    columns='name',
                                    index='citation',
                                    fill_value=-1,
                                    dropna=False )
        return voteTable['vote']

    def _vote_cols(self):
        cols = ()
        for i in range(1,12):
            cols += ('J%d_Vote'%i, 'J%d_Code'%i, 'J%d_Name'%i)
        return cols
