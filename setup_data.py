from __future__ import division
import pandas as pd
import hickle,pickle
import numpy as np
from datetime import datetime

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
    
    print "Overwriting Canadian court information..."
    pickle.dump({'courts':courts},open('canada_full_court_votes.p','w'),-1)

def setup_vinwar(disp=True):
    df = pickle.load(open('vinwar_stata_EDL.xlsx.p','rb'))['df']
    names = np.sort(['mar','fort','gold','bw','stwt','whit','brn','har','mint','clk',
                     'burt','jack','doug','frk','reed','blk','war','rut','mur','vin'])

    print "Unique vote types for column 2"
    print np.unique(df['votetyp2'].dropna())
    # Find merits votes.
    mrtsString = ['MRTS','1RTS']
    meritsIx = ((df['votetyp2']==mrtsString[0]) | (df['votetyp2']==mrtsString[1])).values

    print "Unique vote types for column 2"
    print np.unique(df['votetyp3'].dropna())
    # Find reports votes.
    rptsIx = (df['votetyp3']=='REPT').values

    # Only keep cases where we have either a report or final vote on the merits.
    anyVoteIx = meritsIx | rptsIx
    df = df.ix[anyVoteIx]

    # Collect report votes.
    v = []
    for n in names:
        v.append( df[n+'3r'].values )
    v = np.vstack(v).T
    rptVotes = pd.DataFrame(v,columns=names)

    v = []
    for n in names:
        v.append( df[n+'3dir'].values )
    v = np.vstack(v).T
    rptIdeVotes = pd.DataFrame(v,columns=names)


    # Collect conference votes.
    v = []
    for n in names:
        v.append( df[n+'2r'].values )
    v = np.vstack(v).T
    confVotes = pd.DataFrame(v,columns=names)

    v = []
    for n in names:
        v.append( df[n+'2dir'].values )
    v = np.vstack(v).T
    confIdeVotes = pd.DataFrame(v,columns=names)

    confVoteDate = df['votedat2'].values
    rptVoteDate = df['votedat3'].values
    confVoteDate[confVoteDate=='1582-10-14'] = datetime(1900,1,1,0,0)
    rptVoteDate[rptVoteDate=='1582-10-14'] = datetime(1900,1,1,0,0)

    # Binarize the votes.===========================
    # First remove the nans.
    rptVotes.fillna('',inplace=True)
    rptIdeVotes.fillna('',inplace=True)
    confVotes.fillna('',inplace=True)
    confIdeVotes.fillna('',inplace=True)

    # Make sure we only have binary votes.
    print "Confirm that there are only two kinds of votes."
    print np.unique(rptVotes)
    print np.unique(rptIdeVotes)
    print np.unique(confVotes)
    print np.unique(confIdeVotes)

    # Replace votes.
    # deny/affirm -1, grant/reverse 1
    # conservative -1, liberal 1
    rptVotes.replace('deny/affirm',-1,inplace=True)
    rptVotes.replace('grant/reverse',1,inplace=True)
    rptVotes.replace('',0,inplace=True)

    rptIdeVotes.replace('conservative',-1,inplace=True)
    rptIdeVotes.replace('liberal',1,inplace=True)
    rptIdeVotes.replace('',0,inplace=True)

    confVotes.replace('deny/affirm',-1,inplace=True)
    confVotes.replace('grant/reverse',1,inplace=True)
    confVotes.replace('',0,inplace=True)

    confIdeVotes.replace('conservative',-1,inplace=True)
    confIdeVotes.replace('liberal',1,inplace=True)
    confIdeVotes.replace('',0,inplace=True)


    # =================== 
    # Handle merit votes.
    # =================== 
    # Save extra merit votes in an object array where each column corresponds to particular merits vote where
    # an element, if not -1, contains a vector of len(names) telling us the votes.
    # Find merits votes in columns 3-7.
    mrtVotes = (np.zeros((len(df),5),dtype=int)-1).astype(object)

    for i in xrange(3,8):
        # Find extra merit votes.
        extraMrtsIx = (df['votetyp%d'%i]=='MRTS').values
        print "%d merits votes in col %d"%(extraMrtsIx.sum(),i)
        
        # Create an array for the set of justice votes.
        thisVote = []
        for n in names:
            thisVote.append( df[n+'%dr'%i].values )
        thisVote = np.vstack(thisVote).T
        # Add each row of votes in separately because we are dealing with an object array.
        for ix in np.where(extraMrtsIx)[0]:
            mrtVotes[ix,i-3] = thisVote[ix]
    
    mrtVotes = shift_mrt_votes(mrtVotes)
    if disp:
        show_bad_mrt_votes(mrtVotes)
    
    # Fix these messed up votes.
    fix_bad_mrt_votes(mrtVotes) 

    # Check again.
    if disp:
        show_bad_mrt_votes(mrtVotes)
    
    # Binarize the votes.
    bin_mrt_votes(mrtVotes)


    pickle.dump({'rptVotes':rptVotes,'rptIdeVotes':rptIdeVotes,
                 'confVotes':confVotes,'confIdeVotes':confIdeVotes,
                 'confVoteDate':confVoteDate,'rptVoteDate':rptVoteDate,
                 'mrtVotes':mrtVotes},
                open('warren_conf_votes.p','wb'),-1)


# =================================== #
# Helper function for setup_vinwar(). #
# =================================== #
def bin_mrt_votes(mrtVotes):
    for v in mrtVotes:
        for i in v:
            # Skip missing merit votes.
            if type(i) is int:
                break
            # Replace bad vote data points.
            for ix,j in enumerate(i):
                if j=='grant/reverse':
                    i[ix] = 1
                elif j=='deny/affirm':
                    i[ix] = -1
                else:
                    i[ix] = 0

def fix_bad_mrt_votes(mrtVotes):
    for v in mrtVotes:
        for i in v:
            # Skip missing merit votes.
            if type(i) is int:
                break
            # Replace bad vote data points.
            for ix,j in enumerate(i):
                if j==1 or j=='1.0':
                    i[ix] = 'grant/reverse'
                elif j==2 or j=='2.0':
                    i[ix] = 'deny/affirm'
                elif j=='nan':
                    j = np.nan

def show_bad_mrt_votes(mrtVotes):
    # There seem to some cases where the vote is an integer that is not -1.
    onlyCastVotes = np.vstack([v for v in mrtVotes.ravel() if not type(v) is int]).ravel()
    onlyCastVotes = np.array([v for v in onlyCastVotes if check_vote(v)])

    # According to vinwar_codebook.pdf, grant/reverse=1 and deny/affirm=2.
    print "Problematic votes"
    print np.unique(onlyCastVotes)
    print ""

def check_vote(v):
    """Only return False if we have a value of -1."""
    if type(v) is unicode:
        return True
    elif v==-1:
        return False
    else:
        return True

def shift_mrt_votes(mrtVotes):
    # Push all votes as far to the left the columns as possible for organization.
    for i,r in enumerate(mrtVotes):
        counter = 0
        lastZeroIx = -1
        for counter in range(5):
            if (not type(r[counter]) is int) and (counter>lastZeroIx):
                r[lastZeroIx] = r[counter]
                r[counter] = -1
                lastZeroIx += 1

    # Remove last two columns because there are no votes there anymore.
    # array(map(lambda x:not type(x) is int,mrtVotes[:,3])).sum()
    mrtVotes = mrtVotes[:,:3]
    return mrtVotes



if __name__=='__main__':
    setup_canada()
