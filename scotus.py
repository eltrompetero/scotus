import numpy as np
import pandas as pd
import cPickle as pickle
from warnings import warn
import entropy.entropy as entropy
import os

DATADR = os.path.expanduser('~')+'/Dropbox/Research/py_lib/data_sets/scotus/'
#DATAFILE = 'SCDB_2014_01_justiceCentered_Citation.csv'
DATAFILE = 'SCDB_2015_03_justiceCentered_Citation.csv'
COURT_NAMES = ['waite1','waite2','waite3','FMVinsonVinson','SMintonVinson',         
               'PStewartWarren','AJGoldbergWarren','AFortasWarren','TMarshallWarren', 
               'HABlackmunBurger','WHRehnquistBurger','JPStevensBurger']              
NICE_COURT_NAMES = ['Waite/Waite','Harlan/Waite','Gray/Waite','Vinson/Vinson',       
                    'Minton/Vinson',
                    'Stewart/Warren','Goldberg/Warren','Fortas/Warren','Marshall/Warren',
                    'Blackmun/Burger','Rehnquist/Burger','Stevens/Burger']               
class ScotusData(object):
    @staticmethod
    def rebase_data():
        """
        Reload data from database from supremecourtdatabase.org
        2015-06-27
        """
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

    @staticmethod
    def load_conf_report_votes(courtIx):
        """
        Load set of conference and report votes.
        """
        import scipy.io as sio
        n=9
        # Must load voting data from file to see which votes turn into which.
        name = COURT_NAMES[courtIx]
        indata = sio.loadmat(DATADR+'%s_confra_idevotes' %name)
        for k in indata.keys():
            if k.rfind('all')>=0:
                if k.rfind('conf')>=0:
                    confv = indata[k].astype(float)
                    confv[confv==-1] = np.nan
                else:
                    finv = indata[k].astype(float)
                    finv[finv==-1] = np.nan

        # Take only votes with complete records on both sides.
        fullVotesIx = np.logical_and(np.sum(np.isnan(confv)==0,1)==9, np.sum(np.isnan(finv)==0,1)==9)

        return confv,finv,fullVotesIx

    @staticmethod
    def pairwise_maxent_couplings(courtName,sym,formulation,fullVote=True,conference=False):
        """
        Return solutions to pairwise maxent model.
        2015-08-20

        Params:
        -------
        courtName (str)
            in nice format
        sym (bool)
            True returns solutions on symmetrized data
        formulation (str)
            '0' or '1' for {0,1} and {-1,1}
        fullVote (True,bool)
            in cases where data is missing, return solution on only complete votes if True
        conference (False,bool)
            if need conference votes set True
        """
        if courtName in NICE_COURT_NAMES:
            ix = NICE_COURT_NAMES.index(courtName)
            if conference:
                if sym:
                    Js = pickle.load(open(DATADR+'J_conf_and_report_fullVotes.p','rb'))['JConfSym']
                else:
                    Js = pickle.load(open(DATADR+'J_conf_and_report_fullVotes.p','rb'))['JConf']
            else:
                if sym:
                    Js = pickle.load(open(DATADR+'J_conf_and_report_fullVotes.p','rb'))['JReportSym']
                else:
                    Js = pickle.load(open(DATADR+'J_conf_and_report_fullVotes.p','rb'))['JReport']
            if formulation=='1':
                return entropy.convert_params(Js[ix][:9],Js[ix][9:],concat=True,convertTo='11')
            else:
                return Js[ix]
        return 

if __name__=='__main__':
    scotusdata = ScotusData()
    print "Rebasing data from %s"%DATAFILE
    scotusdata.rebase_data()
