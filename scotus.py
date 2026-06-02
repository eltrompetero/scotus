# ====================================================================================== #
# U.S. Supreme Court voting data.
#
# Two systems live here because both describe the same court:
#   * ScotusData            - modern Supreme Court Database (SCDB) vote tables.
#   * ConferenceReportVotes - historical Warren-era conference vs. report votes.
# Plus the setup routines that build the pickled data these classes read.
#
# Author: Eddie Lee, edlee@alumni.princeton.edu
# ====================================================================================== #
import os
import pickle
import logging

import numpy as np
import pandas as pd

from ._config import DATADR

log = logging.getLogger(__name__)


def _load_legacy_pickle(path):
    """Load a pickle, falling back to latin1 for files written under Python 2."""
    with open(path, 'rb') as f:
        try:
            return pickle.load(f)
        except UnicodeDecodeError:
            f.seek(0)
            return pickle.load(f, encoding='latin1')

# Natural courts for which conference/report .mat files are available, in chronological order.
COURT_NAMES = ['waite1', 'waite2', 'waite3', 'FMVinsonVinson', 'SMintonVinson',
               'PStewartWarren', 'AJGoldbergWarren', 'AFortasWarren', 'TMarshallWarren',
               'HABlackmunBurger', 'WHRehnquistBurger', 'JPStevensBurger']
NICE_COURT_NAMES = ['Waite/Waite', 'Harlan/Waite', 'Gray/Waite', 'Vinson/Vinson',
                    'Minton/Vinson',
                    'Stewart/Warren', 'Goldberg/Warren', 'Fortas/Warren', 'Marshall/Warren',
                    'Blackmun/Burger', 'Rehnquist/Burger', 'Stevens/Burger', 'Connor/Burger',
                    'Scalia/Rehnquist', 'Souter/Rehnquist', 'Thomas/Rehnquist', 'Kennedy/Rehnquist',
                    'Breyer/Rehnquist', 'Kagan/Roberts']
SECOND_REHNQUIST_COURT = ['JPStevens', 'SGBreyer', 'RBGinsburg', 'DHSouter', 'AMKennedy',
                          'SDOConnor', 'WHRehnquist', 'AScalia', 'CThomas']

# The 20 justices that appear in the Warren-era (vinwar) data set, sorted.
WARREN_JUSTICE_NAMES = np.sort(['mar', 'fort', 'gold', 'bw', 'stwt', 'whit', 'brn', 'har',
                                'mint', 'clk', 'burt', 'jack', 'doug', 'frk', 'reed', 'blk',
                                'war', 'rut', 'mur', 'vin'])


class ConferenceReportVotes():
    """Access to paired conference votes on the merits and the final report votes
    for the Warren-era natural courts.
    """
    def __init__(self):
        """
        Fields
        ------
        extra_merit_votes_by_court
            Extra merit votes listed chronologically by column before the final conference vote.
        conference_votes_by_court
            Final conference votes.
        conference_ideology_votes_by_court
            Final conference votes coded by ideological direction.
        report_votes_by_court
            Final report votes.
        report_ideology_votes_by_court
            Final report votes coded by ideological direction.
        justices_by_court
            Justices sitting on each natural court.
        justice_index_by_court
            Column indices of each court's justices in the full vote matrix.
        courts
            List of natural court titles.
        """
        data = _load_legacy_pickle(os.path.join(DATADR, 'warren_conf_votes_bycourt.p'))
        self.extra_merit_votes_by_court = data['mrtVotesByCourt']
        self.conference_votes_by_court = data['confVotesByCourt']
        self.conference_ideology_votes_by_court = data['confIdeVotesByCourt']
        self.justices_by_court = data['justicesByCourt']
        self.report_votes_by_court = data['rptVotesByCourt']
        self.report_ideology_votes_by_court = data['rptIdeVotesByCourt']
        self.justice_index_by_court = data['justiceIxByCourt']

        self.courts = list(self.extra_merit_votes_by_court.keys())

    def conference_and_report(self, court_name, ideological=True, include_extra_merits=False):
        """Return the final conference and report votes for a specified court.

        Parameters
        ----------
        court_name : str
        ideological : bool, True
            If True, return votes coded by ideological direction, else by outcome.
        include_extra_merits : bool, False
            If True, also append the extra merit votes for the court.

        Returns
        -------
        list
            [conference_votes, report_votes] and optionally [..., extra_merit_votes].
        """
        if ideological:
            result = [self.conference_ideology_votes_by_court[court_name],
                      self.report_ideology_votes_by_court[court_name]]
        else:
            result = [self.conference_votes_by_court[court_name],
                      self.report_votes_by_court[court_name]]

        if include_extra_merits:
            result.append(self.extra_merit_votes_by_court[court_name])
        return result


class ScotusData():
    """Wrapper for access to the modern Supreme Court Database (SCDB). Votes are
    considered in the {-1, 1} basis.
    """
    def __init__(self, rebase=False, legacy=False, year=2024):
        """
        Parameters
        ----------
        rebase : bool, False
            Rebuild the cached pickle from the source CSV. Set this when fetching a new year.
        legacy : bool, False
            Load the SCDB legacy data set instead of the modern one.
        year : int, 2024
            Release year of the SCDB modern data set.
        """
        if legacy:
            self.fname = os.path.join(DATADR, 'scotus_table_legacy.p')
            self.datafile = 'SCDB_Legacy_04_justiceCentered_Citation.csv'
        else:
            self.fname = os.path.join(DATADR, 'scotus_table.p')
            self.datafile = f'SCDB_{year}_01_justiceCentered_Citation.csv'

        if (not os.path.isfile(self.fname)) or rebase:
            self.rebase_data()
        try:
            with open(self.fname, 'rb') as f:
                self.table = pickle.load(f)['table']
        except UnicodeDecodeError:
            self.rebase_data()
            with open(self.fname, 'rb') as f:
                self.table = pickle.load(f)['table']
        self.setup_MQ_score()

    def rebase_data(self):
        """Reload the data table from the SCDB CSV (justice-centered citation) and cache it."""
        log.info("Rebasing data from %s...", os.path.join(DATADR, self.datafile))
        table = pd.read_csv(os.path.join(DATADR, self.datafile), encoding='latin1')
        with open(self.fname, 'wb') as f:
            pickle.dump({'table': table}, f, -1)
        log.info("Done.")

    def maj_vote_table(self):
        """Votes of each justice by case with majority orientation."""
        return pd.pivot_table(self.table.loc[:, ('caseId', 'justiceName', 'majority')],
                              columns='justiceName', index='caseId',
                              fill_value=np.nan, dropna=False)

    def dir_vote_table(self):
        """Votes of each justice by case with ideological orientation."""
        return pd.pivot_table(self.table.loc[:, ('caseId', 'justiceName', 'direction')],
                              columns='justiceName', index='caseId',
                              fill_value=np.nan, dropna=False)

    def vote_table(self):
        """Raw vote of each justice by case."""
        return pd.pivot_table(self.table.loc[:, ('caseId', 'justiceName', 'vote')],
                              columns='justiceName', index='caseId',
                              fill_value=np.nan, dropna=False)

    def issue_table(self, detailed=False):
        """Legal issue per case.

        Parameters
        ----------
        detailed : bool, False
            If True, return the specific legal issue, else the broad legal issue area.
        """
        col = 'issue' if detailed else 'issueArea'
        return pd.pivot_table(self.table.loc[:, ('caseId', col)],
                              index='caseId', fill_value=np.nan, dropna=False)

    def term_table(self):
        """SCOTUS term per case."""
        return pd.pivot_table(self.table.loc[:, ('caseId', 'term')],
                              index='caseId', fill_value=np.nan, dropna=False)

    def natural_court(self):
        """Natural court designation per case."""
        return pd.pivot_table(self.table.loc[:, ('caseId', 'naturalCourt')],
                              index='caseId', fill_value=np.nan, dropna=False)

    def justice_names(self):
        """All justice names ordered alphabetically."""
        return np.unique(self.table.justiceName)

    def setup_MQ_score(self):
        """Build the Martin-Quinn ideology score lookup keyed by justice name."""
        df = pd.read_csv(os.path.join(DATADR, 'justices.csv'))
        ref = {}
        for n in np.unique(df['justiceName']):
            mask = (df['justiceName'] == n).values
            ref[n] = (df['term'].iloc[mask].values, df['post_mn'].iloc[mask].values)
        self.mqdict = ref

    def MQ_score(self, name, year=None):
        """Martin-Quinn ideology score for a justice.

        Parameters
        ----------
        name : str
        year : int or None
            If None, return the full (terms, scores) arrays for the justice. Otherwise
            return the single score for that term.
        """
        terms, scores = self.mqdict[name]
        if year is None:
            return terms, scores
        return scores[terms.tolist().index(year)]

    def second_rehnquist_court(self,
                               vote_type='maj',
                               return_case_ix=False,
                               return_justices_ix=False,
                               sorted_by_mq=False,
                               n_voters=9):
        """Voting record for the Second Rehnquist Court (1994-2005). Data set size K=909
        when vote_type='maj'.

        Parameters
        ----------
        vote_type : str, 'maj'
            'maj' (majority orientation) or 'dir' (ideological direction).
        return_case_ix : bool, False
            If True, also return the row indices of the selected cases in the full vote matrix.
        return_justices_ix : bool, False
            If True, also return the column indices of the justices in the vote table.
        sorted_by_mq : bool, False
            If True, sort justice columns from liberal to conservative by Martin-Quinn score.
        n_voters : int, 9
            Number of present voters required. By default only full 9-member votes are kept.

        Returns
        -------
        tuple
            (votes,) plus any requested indices.
        """
        if vote_type == 'maj':
            vote_type = 'majority'
            subTable = self.maj_vote_table()[vote_type][SECOND_REHNQUIST_COURT]
            cols = self.maj_vote_table()[vote_type].columns
        elif vote_type == 'dir':
            vote_type = 'direction'
            subTable = self.dir_vote_table()[vote_type][SECOND_REHNQUIST_COURT]
            cols = self.dir_vote_table()[vote_type].columns
        else:
            raise NotImplementedError

        # Vote codes 1 and 2 mark a cast (majority/minority) vote; only keep cases where the
        # requested number of members cast such a vote.
        if n_voters == 9:
            ix = (((subTable == 1) | (subTable == 2)).sum(1) == n_voters).values
        else:
            # With fewer than 9 members we must also bound by date, using Breyer's appointment
            # and the appointment of Roberts.
            ix = ((pd.to_datetime(self.table.dateDecision) >= '1994-08-03') &
                  (pd.to_datetime(self.table.dateDecision) < '2005-09-29')).values
            case_id = np.unique(self.table['caseId'].loc[ix])

            ix = ((((subTable == 1) | (subTable == 2)).sum(1) == n_voters).values &
                  [i in case_id for i in subTable.index])

        if sorted_by_mq:
            # Reorder the 9 columns from liberal to conservative by Martin-Quinn score.
            subTable = subTable.iloc[:, [0, 2, 3, 1, 5, 4, 6, 7, 8]]

        output = [subTable.loc[ix]]
        if return_case_ix:
            output.append(ix)
        if return_justices_ix:
            output.append(np.array([cols.get_loc(n) for n in SECOND_REHNQUIST_COURT]))
        return tuple(output)

    @staticmethod
    def load_conference_report_votes(court_index):
        """Load conference and report votes for one natural court from its .mat file.

        Parameters
        ----------
        court_index : int
            Index into COURT_NAMES.

        Returns
        -------
        confv : ndarray
        finv : ndarray
        full_votes_ix : ndarray of bool
            Rows with complete records in both conference and report votes.
        """
        import scipy.io as sio

        name = COURT_NAMES[court_index]
        indata = sio.loadmat(os.path.join(DATADR, '%s_confra_idevotes' % name))
        confv = finv = None
        for k in list(indata.keys()):
            if k.rfind('all') >= 0:
                if k.rfind('conf') >= 0:
                    confv = indata[k].astype(float)
                    confv[confv == -1] = np.nan
                else:
                    finv = indata[k].astype(float)
                    finv[finv == -1] = np.nan

        full_votes_ix = np.logical_and((np.isnan(confv) == 0).sum(1) == 9,
                                       (np.isnan(finv) == 0).sum(1) == 9)
        return confv, finv, full_votes_ix

    def october_2015_term(self):
        """October 2015 term during which Scalia died. Data from the SCOTUSblog stat pack.
        +1 is a vote with the majority, -1 against the majority, 0 a recusal. This data is
        already in the SCDB.
        """
        df = pd.read_csv(os.path.join(DATADR, 'stat_pack_october_2015.csv'))
        return df.iloc[:, 1:]


# ====================================================================================== #
# Setup routines that build the pickled Warren-era data.                                 #
# ====================================================================================== #
def setup_warren_votes(disp=True):
    """Build the flat Warren-era conference/report vote pickle (warren_conf_votes.p) from
    the vinwar source. Conference and report votes are binarized (deny/affirm -> -1,
    grant/reverse -> 1, conservative -> -1, liberal -> 1, missing -> 0).
    """
    df = pd.read_pickle(os.path.join(DATADR, 'vinwar_stata_EDL.xlsx.p'))
    names = WARREN_JUSTICE_NAMES

    # Find merits votes.
    merits_strings = ['MRTS', '1RTS']
    merits_ix = ((df['votetyp2'] == merits_strings[0]) |
                 (df['votetyp2'] == merits_strings[1])).values
    # Find report votes.
    report_ix = (df['votetyp3'] == 'REPT').values

    # Only keep cases that have either a report or a final vote on the merits.
    any_vote_ix = merits_ix | report_ix
    df = df.loc[any_vote_ix]

    def _collect(suffix):
        v = np.vstack([df[n + suffix].values for n in names]).T
        return pd.DataFrame(v, columns=names)

    rptVotes = _collect('3r')
    rptIdeVotes = _collect('3dir')
    confVotes = _collect('2r')
    confIdeVotes = _collect('2dir')

    confVoteDate = df['votedat2'].values
    rptVoteDate = df['votedat3'].values
    confVoteDate[confVoteDate == '1582-10-14'] = pd.Timestamp(1900, 1, 1)
    rptVoteDate[rptVoteDate == '1582-10-14'] = pd.Timestamp(1900, 1, 1)

    # Binarize the conference and report votes.
    for votes in (rptVotes, rptIdeVotes, confVotes, confIdeVotes):
        votes.fillna('', inplace=True)
    for votes in (rptVotes, confVotes):
        votes.replace({'deny/affirm': -1, 'grant/reverse': 1, '': 0}, inplace=True)
    for votes in (rptIdeVotes, confIdeVotes):
        votes.replace({'conservative': -1, 'liberal': 1, '': 0}, inplace=True)

    # Save extra merit votes in an object array; each of the (up to 5) columns holds either
    # -1 (no vote) or a vector of len(names) giving the votes for that merit round.
    mrtVotes = (np.zeros((len(df), 5), dtype=int) - 1).astype(object)
    for i in range(3, 8):
        extra_merits_ix = (df['votetyp%d' % i] == 'MRTS').values
        log.info("%d merits votes in col %d", extra_merits_ix.sum(), i)
        this_vote = np.vstack([df[n + '%dr' % i].values for n in names]).T
        for ix in np.where(extra_merits_ix)[0]:
            mrtVotes[ix, i - 3] = this_vote[ix]

    mrtVotes = _shift_merit_votes(mrtVotes)
    if disp:
        _show_bad_merit_votes(mrtVotes)
    _fix_merit_votes(mrtVotes)
    if disp:
        _show_bad_merit_votes(mrtVotes)
    _binarize_merit_votes(mrtVotes)

    with open(os.path.join(DATADR, 'warren_conf_votes.p'), 'wb') as f:
        pickle.dump({'rptVotes': rptVotes, 'rptIdeVotes': rptIdeVotes,
                     'confVotes': confVotes, 'confIdeVotes': confIdeVotes,
                     'confVoteDate': confVoteDate, 'rptVoteDate': rptVoteDate,
                     'mrtVotes': mrtVotes}, f, -1)


def setup_warren_votes_by_court():
    """Split the flat warren_conf_votes.p into per-natural-court matrices and write
    warren_conf_votes_bycourt.p (the file ConferenceReportVotes reads).

    Ported from the prototyping notebook
    "2017-05-09 prototyping import of conference votes.ipynb" (cells 8-13). Two stages:

      1. Group rows by the data set's own natural-court label (natct) to learn which
         justices sat on each named court. The natct labels are known to misidentify some
         courts, so justices voting in fewer than 10% of a court's cases are dropped.
      2. Identify the distinct full 9-justice cohorts directly from the report votes and
         map each back to a named court using the stage-1 membership.

    Run setup_warren_votes() first to produce warren_conf_votes.p.
    """
    names = WARREN_JUSTICE_NAMES

    flat = _load_legacy_pickle(os.path.join(DATADR, 'warren_conf_votes.p'))
    confIdeVotes = flat['confIdeVotes']
    confVotes = flat['confVotes']
    rptIdeVotes = flat['rptIdeVotes']
    rptVotes = flat['rptVotes']
    mrtVotes = flat['mrtVotes']

    # Re-derive the natural-court label per row, aligned with the vote tables above.
    src = pd.read_pickle(os.path.join(DATADR, 'vinwar_stata_EDL.xlsx.p'))
    merits_ix = ((src['votetyp2'] == 'MRTS') | (src['votetyp2'] == '1RTS')).values
    report_ix = (src['votetyp3'] == 'REPT').values
    natct = src['natct'].loc[merits_ix | report_ix].values

    # ---- Stage 1: membership by natct label -----------------------------------------
    natcts = np.unique(pd.Series(natct).dropna())
    natct_justices = {}
    natct_confIde, natct_conf, natct_rptIde, natct_rpt = {}, {}, {}, {}
    for ct in natcts:
        court_ix = (natct == ct)
        natct_confIde[ct] = confIdeVotes.iloc[court_ix].values
        natct_conf[ct] = confVotes.iloc[court_ix].values
        natct_rptIde[ct] = rptIdeVotes.iloc[court_ix].values
        natct_rpt[ct] = rptVotes.iloc[court_ix].values

    for ct in natcts:
        mean_voting_rate = (natct_rptIde[ct] != 0).mean(0)
        # Drop justices who only voted in a handful of this court's cases (natct mislabels).
        weak = (mean_voting_rate < .1) & (mean_voting_rate != 0)
        if weak.any():
            remove_justice_ix = np.where(weak)[0]
            remove_votes = ((natct_rptIde[ct][:, remove_justice_ix] != 0) |
                            (natct_confIde[ct][:, remove_justice_ix] != 0)).ravel()
            log.info("%d votes removed.", remove_votes.sum())
            keep = remove_votes == 0
            natct_rpt[ct] = natct_rpt[ct][keep]
            natct_rptIde[ct] = natct_rptIde[ct][keep]
            natct_conf[ct] = natct_conf[ct][keep]
            natct_confIde[ct] = natct_confIde[ct][keep]
        present = (natct_rptIde[ct] != 0).any(0)
        natct_justices[ct] = names[present]

    with open(os.path.join(DATADR, 'warren_conf_votes_bycourt_natct.p'), 'wb') as f:
        pickle.dump({'confIdeVotesByCourt': natct_confIde, 'rptVotesByCourt': natct_rpt,
                     'confVotesByCourt': natct_conf, 'rptIdeVotesByCourt': natct_rptIde,
                     'justicesByCourt': natct_justices}, f, -1)

    # ---- Stage 2: distinct 9-justice cohorts mapped back to named courts --------------
    fullv = rptVotes.loc[(rptVotes != 0).sum(1) == 9].values.copy()
    fullv[fullv == -1] = 1  # collapse to a 0/1 "did this justice vote" membership mask
    uniq9 = np.unique(fullv, axis=0)

    uniq9_ix = np.vstack([np.where(u)[0] for u in uniq9])
    court_names = []
    natct_keys = list(natct_justices.keys())
    for ix in uniq9_ix:
        matched = ''
        for ct in natct_keys:
            court_ix = np.sort([np.where(j == names)[0] for j in natct_justices[ct]]).ravel()
            if np.array_equal(ix, court_ix):
                matched = ct
                break
        court_names.append(matched)

    confIdeByCourt, rptByCourt, confByCourt, rptIdeByCourt = {}, {}, {}, {}
    mrtByCourt, justicesByCourt, justiceIxByCourt = {}, {}, {}
    for i, u in enumerate(uniq9):
        cols = np.where(u)[0]
        key = str(i)
        confIdeByCourt[key] = confIdeVotes.iloc[:, cols].values
        confByCourt[key] = confVotes.iloc[:, cols].values
        rptIdeByCourt[key] = rptIdeVotes.iloc[:, cols].values
        rptByCourt[key] = rptVotes.iloc[:, cols].values
        justiceIxByCourt[key] = cols
        # Store as a plain ndarray (not a pandas Index) so the pickle is portable across
        # pandas versions.
        justicesByCourt[key] = np.array(rptVotes.columns[u == 1])

    # Keep only complete report votes and subselect the merit votes to each court's justices.
    for i in range(len(uniq9)):
        key = str(i)
        full_vote_ix = (rptIdeByCourt[key] != 0).sum(1) == 9
        rptIdeByCourt[key] = rptIdeByCourt[key][full_vote_ix]
        rptByCourt[key] = rptByCourt[key][full_vote_ix]
        confIdeByCourt[key] = confIdeByCourt[key][full_vote_ix]
        confByCourt[key] = confByCourt[key][full_vote_ix]
        mrtByCourt[key] = mrtVotes[full_vote_ix]
        for v in mrtByCourt[key]:
            for j, vj in enumerate(v):
                if not isinstance(vj, int):
                    v[j] = vj[justiceIxByCourt[key]]

    # Rename anonymous keys to their matched court name; drop cohorts with no match.
    for i, ct in enumerate(court_names):
        key = str(i)
        if ct != '':
            confByCourt[ct] = confByCourt[key]
            confIdeByCourt[ct] = confIdeByCourt[key]
            rptByCourt[ct] = rptByCourt[key]
            rptIdeByCourt[ct] = rptIdeByCourt[key]
            mrtByCourt[ct] = mrtByCourt[key]
            justicesByCourt[ct] = justicesByCourt[key]
            justiceIxByCourt[ct] = justiceIxByCourt[key]
        del confByCourt[key], confIdeByCourt[key]
        del rptByCourt[key], rptIdeByCourt[key]
        del mrtByCourt[key]
        del justicesByCourt[key], justiceIxByCourt[key]

    with open(os.path.join(DATADR, 'warren_conf_votes_bycourt.p'), 'wb') as f:
        pickle.dump({'confIdeVotesByCourt': confIdeByCourt, 'rptVotesByCourt': rptByCourt,
                     'confVotesByCourt': confByCourt, 'rptIdeVotesByCourt': rptIdeByCourt,
                     'mrtVotesByCourt': mrtByCourt, 'justicesByCourt': justicesByCourt,
                     'justiceIxByCourt': justiceIxByCourt}, f, -1)


# =================================== #
# Helpers for setup_warren_votes().   #
# =================================== #
def _binarize_merit_votes(mrtVotes):
    """In-place: grant/reverse -> 1, deny/affirm -> -1, anything else -> 0."""
    for v in mrtVotes:
        for i in v:
            if isinstance(i, int):  # missing merit vote
                break
            for ix, j in enumerate(i):
                if j == 'grant/reverse':
                    i[ix] = 1
                elif j == 'deny/affirm':
                    i[ix] = -1
                else:
                    i[ix] = 0


def _fix_merit_votes(mrtVotes):
    """In-place: convert numeric merit codes back to their canonical strings.
    Per vinwar_codebook.pdf, grant/reverse=1 and deny/affirm=2.
    """
    for v in mrtVotes:
        for i in v:
            if isinstance(i, int):  # missing merit vote
                break
            for ix, j in enumerate(i):
                if j == 1 or j == '1.0':
                    i[ix] = 'grant/reverse'
                elif j == 2 or j == '2.0':
                    i[ix] = 'deny/affirm'
                elif j == 'nan':
                    i[ix] = np.nan


def _show_bad_merit_votes(mrtVotes):
    """Log the unique non-canonical merit vote values for inspection."""
    cast = np.vstack([v for v in mrtVotes.ravel() if not isinstance(v, int)]).ravel()
    cast = np.array([v for v in cast if _is_cast_vote(v)])
    log.info("Problematic merit votes: %s", np.unique(cast))


def _is_cast_vote(v):
    """Return False only for the missing-vote sentinel -1."""
    if isinstance(v, str):
        return True
    return v != -1


def _shift_merit_votes(mrtVotes):
    """Push all cast votes to the leftmost columns, then drop the now-empty trailing two."""
    for r in mrtVotes:
        last_filled_ix = -1
        for counter in range(5):
            if (not isinstance(r[counter], int)) and (counter > last_filled_ix):
                r[last_filled_ix] = r[counter]
                r[counter] = -1
                last_filled_ix += 1
    return mrtVotes[:, :3]


if __name__ == '__main__':
    # For testing.
    ScotusData(rebase=True)
    ScotusData(rebase=True, legacy=True)
