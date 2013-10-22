# -*- coding: utf-8 -*-
"""
    Calculate rca, distance and opportunity gain for industries
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

''' Import statements '''
import csv, sys, os, argparse, math, time
from collections import defaultdict
from os import environ
import pandas as pd
import numpy as np

growth_lib_dir = os.path.abspath('/Users/alexandersimoes/sites/visual_mg/scripts/growth_lib/')
sys.path.append(growth_lib_dir)
import growth

def get_rcas(ybi, geo_level, y):
    ybi = ybi.reset_index(level="year")
    ybi_index = [i for i in ybi.index if len(i[0]) == geo_level and len(i[1]) == 5]
    ybi_index = pd.MultiIndex.from_tuples(ybi_index, names=["bra_id", "isic_id"])
    ybi = ybi.reindex(index=ybi_index)
    
    ybi = ybi.drop(["year", "num_emp", "num_est"], axis=1)
    
    ybi = ybi.unstack()
    levels = ybi.columns.levels
    labels = ybi.columns.labels
    ybi.columns = levels[1][labels[1]]

    '''
        RCAS
    '''
    
    # rcas = growth.rca(ybi).fillna(0)
    rcas = growth.rca(ybi)
    
    rcas_binary = rcas.copy()
    rcas_binary[rcas_binary >= 1] = 1
    rcas_binary[rcas_binary < 1] = 0
    
    '''
        DISTANCES
    '''
    
    '''calculate proximity for opportunity gain calculation'''    
    prox = growth.proximity(rcas_binary)
    '''calculate distances using proximity'''    
    dist = growth.distance(rcas_binary, prox).fillna(0)
    
    '''
        OPP GAIN
    '''
    
    '''calculate product complexity'''
    pci = growth.complexity(rcas_binary)[1]
    '''calculate opportunity gain'''
    opp_gain = growth.opportunity_gain(rcas_binary, prox, pci)
    
    rca_dist_opp = []
    for bra in rcas.index:
        for isic in rcas.columns:
            rca_dist_opp.append([y, bra, isic, rcas[isic][bra], dist[isic][bra], opp_gain[isic][bra]])
    
    return rca_dist_opp

def rca_dist_opp(directory, y):
    
    ybi_file = os.path.abspath(os.path.join(directory, "ybi.tsv"))
    ybi = pd.read_csv(ybi_file, sep="\t", index_col=["year", "bra_id", "isic_id"])
    
    rca_dist_opp = []
    for geo_level in [2, 4, 7, 8]:
        print "geo level:", geo_level
        rca_dist_opp = rca_dist_opp + get_rcas(ybi.copy(), geo_level, y)
    
    
    # now time to merge!
    print "merging datasets..."
    ybi_rdo = pd.DataFrame(rca_dist_opp, columns=["year", "bra_id", "isic_id", "rca", "distance", "opp_gain"])
    ybi_rdo["year"] = ybi_rdo["year"].astype(int)
    ybi_rdo["rca"][ybi_rdo["rca"] == 0] = np.nan
    ybi_rdo = ybi_rdo.set_index(["year", "bra_id", "isic_id"])
    
    # get union of both sets of indexes
    all_ybi_indexes = set(ybi.index).union(set(ybi_rdo.index))
    
    all_ybi_indexes = pd.MultiIndex.from_tuples(all_ybi_indexes, names=["year", "bra_id", "isic_id"])
    # ybi = ybi.reindex(index=all_ybi_indexes, fill_value=0)
    ybi = ybi.reindex(index=all_ybi_indexes)
    ybi["rca"] = ybi_rdo["rca"]
    ybi["distance"] = ybi_rdo["distance"]
    ybi["opp_gain"] = ybi_rdo["opp_gain"]
    
    print ybi.head(10)
    
    # print out file
    print "writing new ybi file..."
    ybi_file = os.path.abspath(os.path.join(directory, "ybi_rcas_dist_opp.tsv"))
    ybi.to_csv(ybi_file, sep="\t", index=True)
        

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--year", help="year for calculations to be run")
    parser.add_argument("-d", "--directory", help="directory for data")
    args = parser.parse_args()
    
    directory = args.directory
    if not directory:
        directory = raw_input("Directory for data files: ")
    
    year = args.year
    if not year:
        year = raw_input("Year for calculations (or all): ")

    rca_dist_opp(directory, year)