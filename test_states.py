# Wrapper for loading US state Supreme Court data.
# Author: Eddie Lee, edlee@alumni.princeton.edu
from .states import *


def test_States():
    states = States('MD')
    v = states.vote_table()
