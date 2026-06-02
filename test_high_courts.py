# ====================================================================================== #
# Smoke tests for international high-court data access.
# Author: Eddie Lee, edlee@alumni.princeton.edu
# ====================================================================================== #
from .high_courts import HighCourt


def test_get_canada():
    courts = HighCourt.get_court('canada')
    assert HighCourt.full_court_size('canada') == 9
    # Each natural court is a dict of justices and a vote matrix sized to the court.
    for court in courts:
        assert court['votes'].shape[1] == len(court['justices'])
