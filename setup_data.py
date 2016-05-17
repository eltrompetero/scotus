from __future__ import division
import pandas as pd
import hickle
import numpy as np

def setup_canada(keepv2=True):
    """2016-05-16"""
    df = pd.read_stata('original_data_files/HCJD_Canada.dta',convert_categoricals=False)

    # Just get the votes. Thes ecolumns end in v1 and v2 (votes on issues 1 and 2).
    colsToKeep = np.sort([c for c in df.columns if ('v1' in c or 'v2' in c)])
    df = df[colsToKeep]
    
    colsToKeep = np.sort([c for c in df.columns if ('v1' in c)])
    if keepv2:
        # Add votes on the second issue as extra votes.
        colsToAddToBottom = np.sort([c for c in df.columns if ('v2' in c)])
        df1 = df[colsToKeep]
        df2 = df[colsToAddToBottom]
        df2.columns = [c.replace('2','1') for c in df2.columns]
        
        df = pd.concat([df1,df2])
    else:
        # Only keep votes on issue 1.
        df = df[colsToKeep]


    # Find all natural courts that had full votes by looking for groups of nine voters. 
    # We can do this by getting the list of sorted justice indices and taking the unique
    # set of 9 voters.
    fullVotesIx = np.where( (np.isnan(df)==0).sum(1)==9 )[0]

    voteStrings = []
    for i in fullVotesIx:
        voteStrings.append( ' '.join([str(i) for i in np.where(np.isnan(df.iloc[i,:])==0)[0]]) )

    voteStrings = np.unique(voteStrings)

    natCourtsIx = []  # justice indices for natural courts
    for v in voteStrings:
        natCourtsIx.append( np.array([int(i) for i in v.split(' ')]) )

    natCourtVotes = []  # votes for natural courts
    for ix in natCourtsIx:
        natCourtVotes.append( df.iloc[:,ix] )

    fullNatCourtVotes = []  # only keeping votes where everyone is voting
    for n in natCourtVotes:
        fullNatCourtVotes.append(n.dropna())

    # Only taking data sets with more than 100 votes...
    courts = []
    for f in fullNatCourtVotes:
        if f.shape[0]>100:
            courts.append({})
            courts[-1]['justices'] = [c.split('_')[0] for c in f.columns]
            courts[-1]['votes'] = f.values
            
    hickle.dump({'courts':courts},open('canada_full_court_votes.hkl','w'))


if __name__=='__main__':
    setup_canada()
