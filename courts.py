# Module for loading supreme court voting data from different databases.
# 2016-05-16
from __future__ import division
import pandas as pd
import hickle,pickle
import os
import numpy as np

DATADR = os.path.expanduser('~')+'/Dropbox/Research/py_lib/data_sets/scotus'

class Courts(object):
    def __init__(self):
        return
    
    @classmethod
    def get_court(self,name):
        if 'can' in name.lower():
            courts = hickle.load(open('%s/%s'%(DATADR,'canada_full_court_votes.hkl'),'r'))['courts']
            return courts
        else:
            raise Exception("Invalid court option.")
    
    @classmethod
    def save_court(self,name,courts):
        if 'can' in name.lower():
            courts = hickle.dump({'courts':courts},
                                 open('%s/%s'%(DATADR,'canada_full_court_votes.hkl'),'w'))
            return courts
        else:
            raise Exception("Invalid court option.")

