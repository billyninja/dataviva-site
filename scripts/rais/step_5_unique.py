# -*- coding: utf-8 -*-
"""
    YB table to add unique industries and occupations to DB
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This script will calculate the number of unique industries (isic)
    and occupations (cbo) that are found in each location (state, meso,
    planning region and municipality).
"""

''' Import statements '''
import csv, sys, os, argparse, math, time, bz2
from collections import defaultdict
from os import environ
from os.path import basename
import pandas as pd
import numpy as np
from ..helpers import get_file
from ..config import DATA_DIR
from ..growth_lib import growth
from scripts import YEAR, DELETE_PREVIOUS_FILE

def get_unique(file, index, column):
    if "o" in file.name:
        tbl = pd.read_csv(file, sep="\t", converters={"cbo_id": str})
    else:
        tbl = pd.read_csv(file, sep="\t")
    tbl = tbl.pivot(index=index, columns=column, values="wage").fillna(0)

    tbl[tbl >= 1] = 1
    tbl[tbl < 1] = 0
    
    return tbl.sum(axis=1)

def main(y, delete_previous_file):
    
    # start with unique isics / bra
    ybi_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybi.tsv'))
    ybi_file = get_file(ybi_file_path)
    yb_unique_isic = get_unique(ybi_file, "bra_id", "isic_id")
    
    # unique cbos / bra
    ybo_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybo.tsv'))
    ybo_file = get_file(ybo_file_path)
    yb_unique_cbo = get_unique(ybo_file, "bra_id", "cbo_id")
    
    yb_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yb.tsv'))
    yb_file = get_file(yb_file_path)
    yb_unique = pd.read_csv(yb_file, sep="\t", index_col=["bra_id"])
    
    yb_unique["unique_isic"] = yb_unique_isic
    yb_unique["unique_cbo"] = yb_unique_cbo
    
    # print out file
    print "writing yb file..."
    new_yb_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yb_uniques.tsv.bz2'))
    yb_unique.to_csv(bz2.BZ2File(new_yb_file_path, 'wb'), sep="\t", index=True)
    
    
    yio_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yio_importance.tsv'))
    yio_file = get_file(yio_file_path)
    # unique cbos / isic
    yi_unique_cbo = get_unique(yio_file, "isic_id", "cbo_id")
        
    # print out file
    yi_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yi.tsv'))
    yi_file = get_file(yi_file_path)
    yi_unique = pd.read_csv(yi_file, sep="\t", index_col="isic_id")
    yi_unique["unique_cbo"] = yi_unique_cbo
    
    print "writing yi file..."
    new_yi_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yi_uniques.tsv.bz2'))
    yi_unique.to_csv(bz2.BZ2File(new_yi_file_path, 'wb'), sep="\t", index=True)
    
    # unique isic / cbo
    yio_file.seek(0)
    yo_unique_isic = get_unique(yio_file, "cbo_id", "isic_id")
    
    # print out file
    yo_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yo.tsv'))
    yo_file = get_file(yo_file_path)
    yo_unique = pd.read_csv(yo_file, sep="\t", converters={"cbo_id":str})
    yo_unique = yo_unique.set_index(["cbo_id"])
    yo_unique["unique_isic"] = yo_unique_isic
    
    print "writing yo file..."
    new_yo_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yo_uniques.tsv.bz2'))
    yo_unique.to_csv(bz2.BZ2File(new_yo_file_path, 'wb'), sep="\t", index=True)
    
    if delete_previous_file:
        print "deleting previous file"
        os.remove(yb_file.name)
        os.remove(yi_file.name)
        os.remove(yo_file.name)
    
if __name__ == "__main__":
    start = time.time()
    
    main(YEAR, DELETE_PREVIOUS_FILE)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;