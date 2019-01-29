# Wrapper for loading US state Supreme Court data.
# Author: Eddie Lee, edlee@alumni.princeton.edu
from .states import *


def test_State():
    state = State('MD')
    v = state.vote_table()
    natCourts = state.extract_nat_courts(v)
    assert sum([i[1] for i in natCourts])==((v>-1).values.sum(1)==len(natCourts[0][0])).sum()
