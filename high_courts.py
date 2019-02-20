# Module for loading supreme court voting data from different databases on national Supreme Courts.
# Author : Eddie Lee, edlee@alumni.princeton.edu
# 2016-05-16
import pandas as pd
import hickle, pickle
import os
import numpy as np
DATADR = os.path.expanduser('~')+'/Dropbox/Research/py_lib/data_sets/scotus'


class Courts():
    def __init__(self):
        return
    
    @classmethod
    def get_court(cls,name):
        if 'can' in name.lower():
            courts = pickle.load(open('%s/%s'%(DATADR,'canada_full_court_votes.p'),'rb'))['courts']
            return courts
        elif 'aus' in name.lower():
            courts = pickle.load(open('%s/%s'%(DATADR,'australian_full_court_votes.p'),'rb'))['courts']
        else:
            raise Exception("Invalid court option.")

        return courts
   
    @classmethod
    def full_court_size(cls, name):
        if 'can' in name.lower():
            return 9
        elif 'aus' in name.lower():
            return 7
        else:
            raise Exception("Invalid court option.")

    @classmethod
    def save_court(self, name, courts):
        if 'can' in name.lower():
            os.remove('%s/%s'%(DATADR,'canada_full_court_votes.p'))
            courts = pickle.dump({'courts':courts},
                                 open('%s/%s'%(DATADR,'canada_full_court_votes.p'),'w'))
        else:
            raise Exception("Invalid court option.")
