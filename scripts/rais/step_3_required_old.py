# -*- coding: utf-8 -*-
"""
    Caculate required numbers YBIO
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The script is the third step in adding a new year of RAIS data to the 
    database. The script will output 1 bzipped TSV files that can then be 
    used throughout the rest of the steps.
    
    The year will needs to be specified by the user, the script will then
    loop through each geographic location to calculation the "required"
    number of employees for this YEAR-LOCATION-INDUSTRY-OCCUPATION combo.
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
from scripts import YEAR, DELETE_PREVIOUS_FILE

def get_ybi_rcas(year, geo_level):
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

def main(year, delete_previous_file):
    
    print "loading YBIO..."
    file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybio.tsv'))
    file = get_file(file_path)
    ybio = pd.read_csv(file, sep="\t", converters={"year": int, "cbo_id":str})
    
    ybio_data = ybio.drop(["year", "wage"], axis=1)
    
    isic_criterion = ybio_data['isic_id'].map(lambda x: len(x) == 5)
    cbo_criterion = ybio_data['cbo_id'].map(lambda x: len(str(x)) == 4)
    
    ybio_required = []
    for geo_level in [2, 4, 7, 8]:
        
        bra_criterion = ybio_data['bra_id'].map(lambda x: len(x) == geo_level)
        ybio_panel = ybio_data[isic_criterion & cbo_criterion & bra_criterion]
        
        ybio_panel = ybio_panel.drop(["num_est", "num_emp"], axis=1)
        ybio_panel = ybio_panel.pivot_table(rows=["bra_id", "cbo_id"], \
                                            cols="isic_id", \
                                            values="num_emp_est")
        
        ybio_panel = ybio_panel.to_panel()
        
        '''get ybi RCAs'''
        ybi_rcas = get_ybi_rcas(year, geo_level)
        ybi_prox = growth.proximity(ybi_rcas.T)
        
        s = time.time()
        geos = list(ybi_rcas.index)
        for geo in geos:
            # print year, geo, time.time() - s
            sys.stdout.write('\r current location: ' + geo + ' ' * 10)
            sys.stdout.flush() # important
            
            s = time.time()
            
            avg_ranks = ybi_prox[geo].rank(ascending=False).dropna()
            
            for isic in ybi_rcas.columns:
                # filter ranking for only geos with RCA by multiplying by 0
                best_isic_matches = ybi_rcas[isic] * avg_ranks
                best_isic_matches = best_isic_matches[best_isic_matches > 0]
                
                if not len(best_isic_matches):
                    continue
                
                # if this location has RCA just use its required
                if best_isic_matches.order().index[0] == geo:
                    required_geos = [geo]
                
                # take top 20%
                num_geos = math.ceil(len(best_isic_matches) *.2)
                required_geos = list(best_isic_matches.order()[:num_geos].index)
                
                required_cbos = ybio_panel[isic].ix[required_geos].fillna(0)
                # print required_cbos
                required_cbos = required_cbos.mean(axis=0)
                required_cbos = required_cbos[required_cbos >= 1]
                # print len(required_cbos)
            
                for cbo in required_cbos.index:
                    ybio_required.append([year, geo, isic, cbo, required_cbos[cbo]])
        
        print
        print "total required rows added:", len(ybio_required)
        
    # merge
    print "merging datasets..."
    ybio_required = pd.DataFrame(ybio_required, columns=["year", "bra_id", "isic_id", "cbo_id", "required"])
    ybio_required['year'] = ybio_required['year'].astype(int)
    # ybio['year'] = ybio['year'].astype(int)
    # ybio = pd.merge(ybio, ybio_required, on=["year", "bra_id", "isic_id", "cbo_id"], how="outer").fillna(0)
    # ybio["year"] = ybio["year_x"]
    # ybio = ybio.drop(["year_y", "year_x"])
    print ybio_required.columns
        
    # print out file
    print "writing to file..."
    new_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybio_just_required.tsv.bz2'))
    ybio_required.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=False)
    
    # if delete_previous_file:
    #     print "deleting previous file"
    #     os.remove(file.name)

if __name__ == "__main__":
    start = time.time()
    
    main(YEAR, DELETE_PREVIOUS_FILE)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;