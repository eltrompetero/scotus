# ====================================================================================== #
# Shared helpers for identifying "natural courts" (cohorts of justices who sat together)
# from voting records. Used by both the data-setup routines (high_courts, scotus) and the
# vote-table accessors (states).
# Author: Eddie Lee, edlee@alumni.princeton.edu
# ====================================================================================== #
import numpy as np


def full_court_vote_sets(df, court_size, min_votes, justice_name_fn):
    """Split a raw vote table into the distinct full-court cohorts that voted together.

    A "natural court" is a unique set of justices who cast a complete vote together. We
    find every row in which exactly ``court_size`` justices voted, take the unique justice
    subsets, and for each subset keep only the cases where all of its members voted.

    Parameters
    ----------
    df : pd.DataFrame
        Raw votes, one column per justice. Missing votes are NaN.
    court_size : int
        Number of justices that constitutes a full court (e.g. 7 for Australia, 9 elsewhere).
    min_votes : int
        Only keep cohorts with strictly more than this many complete votes.
    justice_name_fn : callable
        Maps a column name to a justice label, e.g. ``lambda c: c.split('_')[0]``.

    Returns
    -------
    list of dict
        Each dict has keys 'justices' (list of labels) and 'votes' (ndarray of complete votes).
    """
    full_votes_ix = np.where((np.isnan(df) == 0).sum(1) == court_size)[0]
    assert full_votes_ix.size > 0, "There are no full votes!"

    # Identify each natural court by the sorted set of justice column indices that voted.
    vote_strings = []
    for i in full_votes_ix:
        present = np.where(np.isnan(df.iloc[i, :]) == 0)[0]
        vote_strings.append(' '.join(str(j) for j in present))
    vote_strings = np.unique(vote_strings)

    nat_court_ix = [np.array([int(j) for j in v.split(' ')]) for v in vote_strings]

    courts = []
    for ix in nat_court_ix:
        complete_votes = df.iloc[:, ix].dropna()
        if complete_votes.shape[0] > min_votes:
            courts.append({
                'justices': [justice_name_fn(c) for c in complete_votes.columns],
                'votes': complete_votes.values,
            })
    return courts


def extract_natural_courts(X, only_full_votes=True, threshold_votes='default'):
    """Get indices for unique natural courts identified by unique subsets of voters.

    Parameters
    ----------
    X : ndarray or pd.DataFrame
        Coded vote matrix (e.g. as returned by ``states.State.vote_table``) where values
        in {-1, 0, 1, 2, 3} encode missing/minority/majority/recused/not-participating and
        -1 marks the absence of a vote.
    only_full_votes : bool, True
        If True, only keep cohorts of the maximal observed size (the full court).
    threshold_votes : int or 'default', 'default'
        int specifies a minimum number of votes; 'default' keeps courts with at least
        ``2**n`` votes (n = court size); None/False applies no threshold.

    Returns
    -------
    list of tuple
        (column indices belonging to a natural court, number of complete votes)
    """
    import pandas as pd

    if isinstance(X, pd.DataFrame):
        X = X.values
    if not frozenset(np.unique(X)) <= frozenset((-1, 0, 1, 2, 3)):
        raise ValueError("Vote matrix may only contain the codes -1, 0, 1, 2, 3.")

    nat_courts = [np.where(i)[0].tolist() for i in np.unique(X > -1, axis=0)]
    if only_full_votes:
        mx = max(len(nc) for nc in nat_courts)
        nat_courts = [nc for nc in nat_courts if len(nc) == mx]

    vote_counts = np.zeros(len(nat_courts))
    for i, nc in enumerate(nat_courts):
        vote_counts[i] = (X[:, nc] > -1).all(1).sum()

    if not threshold_votes:
        return list(zip(nat_courts, vote_counts))
    if threshold_votes == 'default':
        assert only_full_votes, \
            "Default threshold_votes can only be used together with only_full_votes."
        return [(nc, c) for nc, c in zip(nat_courts, vote_counts)
                if c >= 2 ** len(nat_courts[0])]
    return [(nc, c) for nc, c in zip(nat_courts, vote_counts) if c >= threshold_votes]
