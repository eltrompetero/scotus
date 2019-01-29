# Wrapper for loading US state Supreme Court data.
# Author: Eddie Lee, edlee@alumni.princeton.edu
import pandas as pd
import numpy as np
import os
DATADR = os.path.expanduser('~')+'/Dropbox/Research/py_lib/data_sets/scotus/'


def list_possible_states():
    files = os.listdir('%s/us_state_court_pickles'%DATADR)
    return sorted([f[:-2] for f in files])


class State():
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

    def vote_table(self, clean=True, return_code=False):
        """Convert default format into a table where rows are individual cases and each
        justice has a column.  Many entries will be empty.
        
        Parameters
        ----------
        clean : bool, True
            If True, returned votes only include 0's and 1's. All other data points are set to -1.
        return_code : bool, False
            If True, return code instead of name.

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
        
        if return_code:
            subTables = []
            for i in range(1,12):
                cols = ('LexisNexisCitationNumber',)+('J%d_Vote'%i, 'J%d_Code'%i)
                subTables.append( df.loc[:,cols] )
                subTables[-1].rename(columns={cols[0]:'citation', cols[1]:'vote', cols[2]:'code'},
                                     inplace=True)
            voteTable = pd.concat(subTables, axis=0)

            voteTable = pd.pivot_table( voteTable,
                                        columns='code',
                                        index='citation',
                                        fill_value=-1,
                                        dropna=False )['vote']

        else:
            subTables = []
            for i in range(1,12):
                cols = ('LexisNexisCitationNumber',)+('J%d_Vote'%i, 'J%d_Name'%i)
                subTables.append( df.loc[:,cols] )
                subTables[-1].rename(columns={cols[0]:'citation', cols[1]:'vote', cols[2]:'name'},
                                     inplace=True)
            voteTable = pd.concat(subTables, axis=0)

            # address some data coding bugs
            if self.state=='ID':
                voteTable.loc[(voteTable['name']=='Jones').values,'name'] = 'J. Jones'
            elif self.state=='MD':
                voteTable.loc[(voteTable['name']=='J. Murrphy').values,'name'] = 'J. Murphy'
            elif self.state=='OH':
                voteTable.loc[(voteTable['name']=='Oconnor').values,'name'] = 'OConnor'
            elif self.state=='MI':
                voteTable.loc[(voteTable['name']=='294').values,'name'] = 'Brickley'
                voteTable.loc[(voteTable['name']=='581').values,'name'] = 'Taylor'
            elif self.state=='KS':
                voteTable.loc[(voteTable['name']=='Gernon ').values,'name'] = 'Gernon'
            elif self.state=='SC':
                voteTable.loc[(voteTable['name']=='Burrnett').values,'name'] = 'Burnett'

            voteTable = pd.pivot_table( voteTable,
                                        columns='name',
                                        index='citation',
                                        fill_value=-1,
                                        dropna=False )['vote']

            # Throw out any blank justice names
            voteTable = voteTable.loc[:,voteTable.columns!='']

        if not clean:
            return voteTable

        voteTable[((voteTable!=0)|(voteTable!=1)).values] = -1
        return voteTable

    def _vote_cols(self):
        cols = ()
        for i in range(1,12):
            cols += ('J%d_Vote'%i, 'J%d_Code'%i, 'J%d_Name'%i)
        return cols
