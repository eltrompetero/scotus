# ====================================================================================== #
# Shared configuration for the scotus data-sets package.
# Author: Eddie Lee, edlee@alumni.princeton.edu
# ====================================================================================== #
import os

# Directory holding the raw and pickled court data. Override with the SCOTUS_DATA_DIR
# environment variable; otherwise fall back to the default research location.
DATADR = os.environ.get(
    "SCOTUS_DATA_DIR",
    os.path.expanduser("~/Dropbox/Research/data_sets/scotus"),
)
