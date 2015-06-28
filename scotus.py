import numpy as np
import pandas as pd
import cPickle as pickle
from warnings import warn

DATADR = '/Users/eddie/Dropbox/Research/py_lib/data_sets/scotus/'
DATAFILE = 'SCDB_2014_01_justiceCentered_Citation.csv'

class ScotusData():
    @staticmethod
    def rebase_data():
        """2015-06-27"""
        warn("This may be memory intensive.")

        table = pd.read_csv(DATADR+DATAFILE)
        
        # Recase table to be caseid by justice vote.
        # Get the part of the table that we wish to pivot.
        subTable = table.loc[:,['caseId','justiceName','vote']]
        voteTable = pd.pivot_table( subTable, columns='justiceName', index='caseId', fill_value=np.nan )

        subTable = table.loc[:,['caseId','justiceName','direction']]
        dirVoteTable = pd.pivot_table( subTable, columns='justiceName', index='caseId', fill_value=np.nan )

        subTable = table.loc[:,['caseId','justiceName','majority']]
        majVoteTable = pd.pivot_table( subTable, columns='justiceName', index='caseId', fill_value=np.nan )
        
        justiceNames = np.unique(table.justiceName)

        pickle.dump( {'justiceNames':justiceNames}, open(DATADR+'justiceNames.p','wb') )
        pickle.dump( {'voteTable':voteTable,'dirVoteTable':dirVoteTable,'majVoteTable':majVoteTable},
                    open(DATADR+'vote_tables.p','wb'))

    @staticmethod
    def majVoteTable():
        return pickle.load(open(DATADR+'vote_tables.p','rb'))['majVoteTable']

    @staticmethod
    def dirVoteTable():
        return pickle.load(open(DATADR+'vote_tables.p','rb'))['dirVoteTable']
    
    @staticmethod
    def voteTable():
        return pickle.load(open(DATADR+'vote_tables.p','rb'))['voteTable']

    @staticmethod
    def justice_names():
        return pickle.load(open(DATADR+'justiceNames.p','rb'))['justiceNames']

