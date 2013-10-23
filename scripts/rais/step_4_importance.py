# -*- coding: utf-8 -*-
"""
    Calculate exclusivity for a given occupation in a given year
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

''' Import statements '''
import csv, sys, os, argparse, math, time, bz2
from collections import defaultdict
from os import environ
import pandas as pd
import numpy as np
from ..helpers import get_file
from ..config import DATA_DIR
from ..growth_lib import growth

def get_all_cbo(y):
    yo_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybo.tsv'))
    yo_file_path = get_file(yo_file_path)

    yo = pd.read_csv(yo_file_path, sep="\t", converters={"cbo_id":str})
    yo = yo.set_index(["cbo_id"])
    cbos = [cbo for cbo in list(yo.index) if len(cbo) == 4]
    
    return cbos

def get_ybi_rcas(geo_level):
    ybi_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybi.tsv'))
    ybi_file_path = get_file(ybi_file_path)
    ybi = pd.read_csv(ybi_file_path, sep="\t")
    
    isic_criterion = ybi['isic_id'].map(lambda x: len(x) == 5)
    bra_criterion = ybi['bra_id'].map(lambda x: len(x) == geo_level)
    
    ybi = ybi[isic_criterion & bra_criterion]
    ybi = ybi.drop(["year", "num_emp", "num_est"], axis=1)
    
    ybi = ybi.pivot(index="bra_id", columns="isic_id", values="wage").fillna(0)
    
    rcas = growth.rca(ybi)
    rcas[rcas >= 1] = 1
    rcas[rcas < 1] = 0
    
    return rcas

def calculate_exclusivity(y):
    start = time.time()
    all_cbo = get_all_cbo(y)
    
    '''get ybi RCAs'''
    rcas = get_ybi_rcas(8)
    
    denoms = rcas.sum()
    
    print "loading YBIO..."
    ybio_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybio.tsv'))
    ybio_file_path = get_file(ybio_file_path)
    
    ybio = pd.read_csv(ybio_file_path, sep="\t", converters={"year": str, "cbo_id":str})
    isic_criterion = ybio['isic_id'].map(lambda x: len(x) == 5)
    cbo_criterion = ybio['cbo_id'].map(lambda x: len(x) == 4)
    bra_criterion = ybio['bra_id'].map(lambda x: len(x) == 8)
    ybio = ybio[isic_criterion & cbo_criterion & bra_criterion]
    
    ybio = ybio.drop(["num_est", "wage"], axis=1)
    print "pivoting YBIO..."
    ybio = ybio.pivot_table(rows=["isic_id", "cbo_id"], cols="bra_id", values="num_emp")
    ybio = ybio.fillna(0)
    
    panel = ybio.to_panel()
    panel = panel.swapaxes("items", "minor")
    panel = panel.swapaxes("major", "minor")
  
    # z       = occupations
    # rows    = bras
    # colums  = isics
    
    print y, time.time() - start
    # sys.exit()
    yio_importance = []
    for cbo in all_cbo:
        start = time.time()
        
        try:
            num_emp = panel[cbo].fillna(0)
        except:
            continue
        numerators = num_emp * rcas
        numerators = numerators.fillna(0)
        
        '''convert nominal num_emp values to 0s and 1s'''
        numerators[numerators >= 1] = 1
        numerators[numerators < 1] = 0
        
        numerators = numerators.sum()
        importance = numerators / denoms
        # print importance
        
        for isic in importance.index:
            imp = importance[isic]
            yio_importance.append([y, isic, cbo, imp])
        
        print y, cbo, time.time() - start
    
    # now time to merge!
    print "merging datasets..."
    yio_importance = pd.DataFrame(yio_importance, columns=["year", "isic_id", "cbo_id", "importance"])
    yio_importance = yio_importance.set_index(["year", "isic_id", "cbo_id"])
    
    yio_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yio.tsv'))
    yio_file_path = get_file(yio_file_path)
    
    yio = pd.read_csv(yio_file_path, sep="\t", converters={"year": str, "cbo_id": str})
    yio = yio.set_index(["year", "isic_id", "cbo_id"])
    yio["importance"] = yio_importance["importance"]
        
    # print out file
    print "writing to file..."
    new_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yio_importance.tsv.bz2'))
    yio.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)

if __name__ == "__main__":
    start = time.time()
    
    # Get path of the file from the user
    help_text_year = "the year of data being converted "
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--year", help=help_text_year)
    args = parser.parse_args()
    
    year = args.year
    if not year:
        year = raw_input(help_text_year)
    
    calculate_exclusivity(year)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;