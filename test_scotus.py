# =============================================================================================== #
# Test module for loading modern SCOTUS voting data.
# Author: Eddie Lee, edl56@cornell.edu
# =============================================================================================== #
from .scotus import *

def test_ScotusData():
    scotus = ScotusData()

    # check that all the data tables are the same size
    shape = scotus.vote_table().shape
    assert np.array_equal(shape, scotus.dir_vote_table().shape)
    assert np.array_equal(shape, scotus.maj_vote_table().shape)
    assert shape[0]==len(scotus.term_table())
    assert shape[0]==len(scotus.term_table())
    assert shape[0]==len(scotus.natural_court())
    assert shape[0]==len(scotus.issue_table())
