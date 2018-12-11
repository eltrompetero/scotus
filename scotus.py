# =============================================================================================== #
# Module for loading modern SCOTUS voting data.
# Author: Eddie Lee, edl56@cornell.edu
# =============================================================================================== #
import numpy as np
import pandas as pd
import pickle
from warnings import warn
import entropy.entropy as entropy
import os

DATADR = os.path.expanduser('~')+'/Dropbox/Research/py_lib/data_sets/scotus/'
COURT_NAMES = ['waite1','waite2','waite3','FMVinsonVinson','SMintonVinson',         
               'PStewartWarren','AJGoldbergWarren','AFortasWarren','TMarshallWarren', 
               'HABlackmunBurger','WHRehnquistBurger','JPStevensBurger']              
NICE_COURT_NAMES = ['Waite/Waite','Harlan/Waite','Gray/Waite','Vinson/Vinson',       
                    'Minton/Vinson',
                    'Stewart/Warren','Goldberg/Warren','Fortas/Warren','Marshall/Warren',
                    'Blackmun/Burger','Rehnquist/Burger','Stevens/Burger','Connor/Burger',
                    'Scalia/Rehnquist','Souter/Rehnquist','Thomas/Rehnquist','Kennedy/Rehnquist','Breyer/Rehnquist',
                    'Kagan/Roberts']
SECOND_REHNQUIST_COURT=['JPStevens','SGBreyer','RBGinsburg','DHSouter','AMKennedy',
                        'SDOConnor','WHRehnquist','AScalia','CThomas']


class ConfVotesData(object):
    def __init__(self):
        """
        For easy access to pairs of conference votes on the merits and the final report votes.

        Fields:
        -------
        mrtVotesByCourt
            Extra merit votes listed chronologically by column before the final conference vote.
        confVotesByCourt
            Final conference votes.
        confIdeVotesByCourt
        rptVotesByCourt
        rptIdeVotesByCourt
        justicesByCourt
        courts
            List of natural court titles.

        Methods:
        --------
        conf_rpt()
        """
        data = pickle.load(open(DATADR+'/warren_conf_votes_bycourt.p','rb'))
        self.mrtVotesByCourt = data['mrtVotesByCourt']
        self.confVotesByCourt = data['confVotesByCourt']
        self.confIdeVotesByCourt = data['confIdeVotesByCourt']
        self.justicesByCourt = data['justicesByCourt']
        self.rptVotesByCourt = data['rptVotesByCourt']
        self.rptIdeVotesByCourt = data['rptIdeVotesByCourt']
        self.justiceIxByCourt = data['justiceIxByCourt']
        
        self.courts = list(self.mrtVotesByCourt.keys())
    
    def conf_rpt(self,ctName,ide=True,extraConf=False):
        """
        Return a pair of final conference and report votes for a specified court.

        Params:
        -------
        ctName (str)
        ide (bool=True)
        extraConf (bool=False)
        """

        if ide:
            toReturn = [self.confIdeVotesByCourt[ctName],self.rptIdeVotesByCourt[ctName]]
        else:
            toReturn = [self.confVotesByCourt[ctName],self.rptVotesByCourt[ctName]]

        if extraConf:
            toReturn.append(self.mrtVotesByCourt[ctName])
        return toReturn


class ScotusData():
    """Wrapper for access to modern SCDB downloaded in 2017 (?). Votes are considered in the {-1,1} basis.
    """
    def __init__(self,rebase=False,legacy=False):
        if legacy:
            self.fname = DATADR+'scotus_table_legacy.p'
            self.datafile = 'SCDB_Legacy_04_justiceCentered_Citation.csv'
        else:
            self.fname = DATADR+'scotus_table.p'
            self.datafile = 'SCDB_2016_01_justiceCentered_Citation.csv'

        if (not os.path.isfile(self.fname)) or rebase:
            self.rebase_data()
        try:
            self.table = pickle.load(open(self.fname,'rb'))['table']
        except UnicodeDecodeError:
            self.rebase_data()
            self.table = pickle.load(open(self.fname,'rb'))['table']
        self.setup_MQ_score()

    def rebase_data(self):
        """
        Reload data from database from supremecourtdatabase.org
        """

        print("Rebasing data from %s..."%DATADR+self.datafile)
        table = pd.read_csv(DATADR+self.datafile, encoding='latin1')
        pickle.dump({'table':table}, open(self.fname,'wb'), -1)
        print("Done.")
    
    def maj_vote_table(self):
        """Votes of each justice by case with majority orientation."""
        majVoteTable = pd.pivot_table( self.table.loc[:,('caseId','justiceName','majority')],
                                       columns='justiceName', index='caseId', fill_value=np.nan, dropna=False )
        return majVoteTable

    def dir_vote_table(self):
        """Votes of each justice by case with ideological orientation."""
        dirVoteTable = pd.pivot_table( self.table.loc[:,('caseId','justiceName','direction')],
                                       columns='justiceName', index='caseId', fill_value=np.nan, dropna=False )
        return dirVoteTable
    
    def vote_table(self):
        voteTable = pd.pivot_table( self.table.loc[:,('caseId','justiceName','vote')],
                                    columns='justiceName', index='caseId', fill_value=np.nan, dropna=False )
        return voteTable
    
    def issue_table(self, detailed=False):
        """
        Parameters
        ----------
        detailed : bool, False
            If True, return specific legal issue otherwise return broad legal issue.
        """

        if detailed:
            issueTable = pd.pivot_table( self.table.loc[:,('caseId','issue')],
                                         index='caseId', fill_value=np.nan, dropna=False )
        else:
            issueTable = pd.pivot_table( self.table.loc[:,('caseId','issueArea')],
                                         index='caseId', fill_value=np.nan, dropna=False )
        return issueTable

    def term_table(self):
        termTable = pd.pivot_table( self.table.loc[:,('caseId','term')],
                                    index='caseId', fill_value=np.nan, dropna=False )
        return termTable

    def natural_court(self):
        natCourtTable = pd.pivot_table( self.table.loc[:,('caseId','naturalCourt')],
                                        index='caseId', fill_value=np.nan, dropna=False )
        return natCourtTable

    def justice_names(self):
        justiceNames = np.unique(self.table.justiceName)
        return justiceNames
    
    def setup_MQ_score(self):
        df = pd.read_csv('%s/%s'%(DATADR,'justices.csv'))
        ref = {}
        for n in np.unique(df['justiceName']):
            ref[n] = df['post_mn'].iloc[(df['justiceName']==n).values].values
        self.mqdict = ref

    def MQ_score(self,name):
        return self.mqdict.get(name,None)

    def second_rehnquist_court(self, return_case_ix=False, return_justices_ix=False):
        """
        Parameters
        ----------
        return_case_ix : bool, False
            If True, also return the indices of the full vote matrix that correspond to the subset
            of cases that we selected out for the Second Rehnquist Court.
        return_justices_ix : bool, False
            If True, return the index of the justices in the columns of the vote table.

        Returns
        -------
        tuple
        """

        subTable=self.maj_vote_table()['majority'][SECOND_REHNQUIST_COURT]
        ix=(( (subTable==1)|(subTable==2) ).sum(1)==9).values
        
        output=[subTable.iloc[ix]]
        if return_case_ix:
            output.append(ix)
        if return_justices_ix:
            output.append(np.array([self.maj_vote_table()['majority'].columns.get_loc(n)
                                    for n in SECOND_REHNQUIST_COURT]))
        return tuple(output)

    @staticmethod
    def load_conf_report_votes(courtIx):
        """
        Load set of conference and report votes.

        Params:
        -------
        courtIx (int)

        Value:
        ------
        confvotes
        reportvotes
        fullVotesIx
            Votes with full votes in both conference and report votes.
        """
        import scipy.io as sio
        n=9
        # Must load voting data from file to see which votes turn into which.
        name = COURT_NAMES[courtIx]
        indata = sio.loadmat(DATADR+'%s_confra_idevotes' %name)
        for k in list(indata.keys()):
            if k.rfind('all')>=0:
                if k.rfind('conf')>=0:
                    confv = indata[k].astype(float)
                    confv[confv==-1] = np.nan
                else:
                    finv = indata[k].astype(float)
                    finv[finv==-1] = np.nan

        # Get index of votes with complete records on both sides.
        fullVotesIx = np.logical_and(np.sum(np.isnan(confv)==0,1)==9, np.sum(np.isnan(finv)==0,1)==9)

        return confv,finv,fullVotesIx

    def oct2015(self):
        """
        Load October 2015 term during which Scalia died. Data from scotus blog statpack.
        +1 is vote with majority and -1 is vote against majority. 0 is recusal.

        This data is already in the SCDB.
        """
        df = pd.read_csv('%s/%s'%(DATADR,'stat_pack_october_2015.csv'))
        return df.iloc[:,1:]

if __name__=='__main__':
    scotusdata = ScotusData(rebase=True)
    scotusdata = ScotusData(rebase=True,legacy=True)
