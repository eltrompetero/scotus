# Wrapper for loading US state Supreme Court data.
# Author: Eddie Lee, edlee@alumni.princeton.edu
from .states import *


def test_State():
    state = State('MD')
    v = state.vote_table()
