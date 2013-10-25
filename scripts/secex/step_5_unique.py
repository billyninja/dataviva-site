# -*- coding: utf-8 -*-
"""
    YB table to add unique industries and occupations to DB
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This script will calculate the number of unique products (hs)
    and destinations (wld) that are found in each location (state, meso,
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

def get_unique(file, index, column):
    if "p" in file.name:
        tbl = pd.read_csv(file, sep="\t", converters={"hs_id": str})
    else:
        tbl = pd.read_csv(file, sep="\t")
    tbl = tbl.pivot(index=index, columns=column, values="val_usd").fillna(0)

    tbl[tbl >= 1] = 1
    tbl[tbl < 1] = 0
    
    return tbl.sum(axis=1)

def uniques(y, delete_previous_file):
    
    # start with unique hs / bra
    ybp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'ybp.tsv'))
    ybp_file = get_file(ybp_file_path)
    yb_unique_hs = get_unique(ybp_file, "bra_id", "hs_id")
    
    # unique wld / bra
    ybw_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'ybw.tsv'))
    ybw_file = get_file(ybw_file_path)
    yb_unique_wld = get_unique(ybw_file, "bra_id", "wld_id")
    
    yb_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yb_ecis.tsv'))
    yb_file = get_file(yb_file_path)
    yb_unique = pd.read_csv(yb_file, sep="\t", index_col=["bra_id"])
    
    yb_unique["unique_hs"] = yb_unique_hs
    yb_unique["unique_wld"] = yb_unique_wld
    
    # print out file
    print "writing yb file..."
    new_yb_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yb_ecis_uniques.tsv.bz2'))
    yb_unique.to_csv(bz2.BZ2File(new_yb_file_path, 'wb'), sep="\t", index=True)
    
    
    ypw_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'ypw.tsv'))
    ypw_file = get_file(ypw_file_path)
    # unique cbos / isic
    yp_unique_wld = get_unique(ypw_file, "hs_id", "wld_id")
        
    # print out file
    yp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yp_pcis.tsv'))
    yp_file = get_file(yp_file_path)
    yp_unique = pd.read_csv(yp_file, sep="\t", index_col="hs_id")
    yp_unique["unique_wld"] = yp_unique_wld
    
    print "writing yp file..."
    new_yp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yp_pcis_uniques.tsv.bz2'))
    yp_unique.to_csv(bz2.BZ2File(new_yp_file_path, 'wb'), sep="\t", index=True)
    
    # unique isic / cbo
    ypw_file.seek(0)
    yw_unique_hs = get_unique(ypw_file, "wld_id", "hs_id")
    
    # print out file
    yw_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yw_ecis.tsv'))
    yw_file = get_file(yw_file_path)
    yw_unique = pd.read_csv(yw_file, sep="\t", converters={"cbo_id":str})
    yw_unique = yw_unique.set_index(["wld_id"])
    yw_unique["unique_hs"] = yw_unique_hs
    
    print "writing yo file..."
    new_yw_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yw_ecis_uniques.tsv.bz2'))
    yw_unique.to_csv(bz2.BZ2File(new_yw_file_path, 'wb'), sep="\t", index=True)
    
    if delete_previous_file:
        print "deleting previous file"
        os.remove(yb_file.name)
        os.remove(yp_file.name)
        os.remove(yw_file.name)
    
if __name__ == "__main__":
    start = time.time()
    
    # Get path of the file from the user
    help_text_year = "the year of data being converted "
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--year", help=help_text_year)
    parser.add_argument("-d", "--delete", action='store_true', default=False)
    args = parser.parse_args()
    
    delete_previous_file = args.delete
    
    year = args.year
    if not year:
        year = raw_input(help_text_year)
    
    uniques(year, delete_previous_file)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;