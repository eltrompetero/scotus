import numpy as np
import pandas as pd
import pickle as pickle
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


class ScotusData(object):
    def __init__(self,rebase=False,legacy=False):
        if legacy:
            self.fname = DATADR+'scotus_table_legacy.p'
            self.datafile = 'SCDB_Legacy_04_justiceCentered_Citation.csv'
        else:
            self.fname = DATADR+'scotus_table.p'
            self.datafile = 'SCDB_2016_01_justiceCentered_Citation.csv'

        if (not os.path.isfile(self.fname)) or rebase:
            self.rebase_data()
        self.table = pickle.load(open(self.fname,'rb'))['table']
        self.setup_MQ_score()

    def rebase_data(self):
        """
        Reload data from database from supremecourtdatabase.org
        """
        table = pd.read_csv(DATADR+self.datafile)
        pickle.dump({'table':table},open(self.fname,'wb'),-1)
    
    def majVoteTable(self):
        subTable = self.table.loc[:,['caseId','justiceName','majority']]
        majVoteTable = pd.pivot_table( subTable, columns='justiceName', index='caseId', fill_value=np.nan )
        return majVoteTable

    def dirVoteTable(self):
        subTable = self.table.loc[:,['caseId','justiceName','direction']]
        dirVoteTable = pd.pivot_table( subTable, columns='justiceName', index='caseId', fill_value=np.nan )
        return dirVoteTable
    
    def voteTable(self):
        subTable = self.table.loc[:,['caseId','justiceName','vote']]
        voteTable = pd.pivot_table( subTable, columns='justiceName', index='caseId', fill_value=np.nan )
        return voteTable
    
    def issueTable(self,detailed=False):
        """
        Parameters
        ----------
        detailed : bool,False
            If True, return specific legal issue otherwise return broad legal issue.
        """
        if detailed:
            issueTable = self.table.loc[:,['caseId','issue']]
        else:
            issueTable = self.table.loc[:,['caseId','issueArea']]
        issueTable = pd.pivot_table( issueTable,index='caseId',fill_value=np.nan )
        return issueTable

    def termTable(self):
        termTable = self.table.loc[:,['caseId','term']]
        termTable = pd.pivot_table( termTable,index='caseId',fill_value=np.nan )
        return termTable

    def naturalCourt(self):
        natCourtTable = self.table.loc[:,['caseId','naturalCourt']]
        natCourtTable = pd.pivot_table( natCourtTable,index='caseId',fill_value=np.nan )
        return natCourtTable


    def justice_names(self):
        justiceNames = np.unique(self.table.justiceName)
        return justiceNames
    
    def setup_MQ_score(self):
        df = pd.read_csv('%s/%s'%(DATADR,'justices.csv'))
        ref = {}
        for n in np.unique(df['justiceName']):
            ref[n] = df['post_mn'].ix[df['justiceName']==n].values
        self.mqdict = ref

    def MQ_score(self,name):
        return self.mqdict.get(name,None)

    def second_rehnquist_court(self,return_case_ix=False,return_justices_ix=False):
        """
        Parameters
        ----------
        return_case_ix : bool,False
            If True, also return the indices of the full vote matrix that correspond to the subset
            of cases that we selected out for the Second Rehnquist Court.
        return_justices_ix : bool,FAlse
            If True, return the index of the justices in the columns of the vote table.
        """
        subTable=self.majVoteTable()['majority'][SECOND_REHNQUIST_COURT]
        ix=(( (subTable==1)|(subTable==2) ).sum(1)==9).values
        
        output=[subTable.iloc[ix]]
        if return_case_ix:
            output.append(ix)
        if return_justices_ix:
            output.append(np.array([self.majVoteTable()['majority'].columns.get_loc(n)
                                    for n in SECOND_REHNQUIST_COURT]))
        return output

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

        Returns:
        --------
        J
        """
        if type(formulation) is int:
            formulation = str(formulation)

        if courtName in ['Waite/Waite','Harlan/Waite','Gray/Waite','Vinson/Vinson',       
                         'Minton/Vinson',
                         'Stewart/Warren','Goldberg/Warren','Fortas/Warren','Marshall/Warren',
                         'Blackmun/Burger','Rehnquist/Burger','Stevens/Burger']:
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
        elif courtName in NICE_COURT_NAMES:
            J = pickle.load(open(DATADR+'J_by_court.p','rb'))[''.join(courtName.split('/'))]
            if conference:
                if sym:
                    J = J['JconfSym']
                else:
                    J = J['Jconf']
            else:
                if sym:
                    J = J['JReportSym']
                else:
                    J = J['JReport']
            if formulation=='1':
                return entropy.convert_params(J[:9],J[9:],concat=True,convertTo='11')
            else:
                return J
        else:
            raise Exception("Court not found.")
    
    @classmethod
    def save_couplings(self,name,couplings):
        """
        2016-06-03

        Params:
        -------
        name (str)
            String identifying the court. Should include a forward slash between junior and chief justices.
        couplings (dict)
            Dictionary with fields 'JConf', 'JConfSym', 'JReport', 'JReportSym'
        """
        print("Make sure to append court to the end of NICE_COURT_NAMES.")
        solns = pickle.load(open(DATADR+'J_by_court.p','rb'))
        solns[name] = couplings
        pickle.dump(solns,open(DATADR+'J_by_court.p','wb'),-1)

    def oct2015(self):
        """
        Load October 2015 term during which Scalia died. Data from scotus blog statpack.
        +1 is vote with majority and -1 is vote against majority. 0 is recusal.

        This data is already in the SCDB.
        """
        df = pd.read_csv('%s/%s'%(DATADR,'stat_pack_october_2015.csv'))
        return df.ix[:,1:]

if __name__=='__main__':
    print("Rebasing data from %s"%DATAFILE)
    scotusdata = ScotusData(rebase=True)
