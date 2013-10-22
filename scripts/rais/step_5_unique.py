# -*- coding: utf-8 -*-
"""
    YB table to add unique industries and occupations to DB
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This script will calculate the number of unique industries (isic)
    and occupations (cbo) that are found in each location (state, meso,
    planning region and municipality).
"""

''' Import statements '''
import csv, sys, os, argparse, math, time
from collections import defaultdict
from os import environ
import pandas as pd
import numpy as np

def get_unique(file, index, column):
    if "o" in file:
        tbl = pd.read_csv(file, sep="\t", converters={"cbo_id": str})
    else:
        tbl = pd.read_csv(file, sep="\t")
    tbl = tbl.pivot(index=index, columns=column, values="wage").fillna(0)

    tbl[tbl >= 1] = 1
    tbl[tbl < 1] = 0
    
    return tbl.sum(axis=1)

def uniques(dir, y):
    
    # start with unique isics / bra
    ybi_file = os.path.abspath(os.path.join(directory, "ybi.tsv"))
    yb_unique_isic = get_unique(ybi_file, "bra_id", "isic_id")
    
    # unique cbos / bra
    ybo_file = os.path.abspath(os.path.join(directory, "ybo.tsv"))
    yb_unique_cbo = get_unique(ybo_file, "bra_id", "cbo_id")
    # print yb_unique_cbo
    # sys.exit()
    
    yb_file = os.path.abspath(os.path.join(directory, "yb.tsv"))
    yb_unique = pd.read_csv(yb_file, sep="\t", index_col=["bra_id"])
    
    yb_unique["unique_isic"] = yb_unique_isic
    yb_unique["unique_cbo"] = yb_unique_cbo
    
    # print out file
    print "writing yb file..."
    yb_file = os.path.abspath(os.path.join(directory, "yb_uniques.tsv"))
    yb_unique.to_csv(yb_file, sep="\t", index=True)
    
    
    
    yio_file = os.path.abspath(os.path.join(directory, "yio.tsv"))
    # unique cbos / isic
    yi_unique_cbo = get_unique(yio_file, "isic_id", "cbo_id")
        
    # print out file
    yi_file = os.path.abspath(os.path.join(directory, "yi.tsv"))
    yi_unique = pd.read_csv(yi_file, sep="\t", index_col="isic_id")
    yi_unique["unique_cbo"] = yi_unique_cbo
    
    print "writing yi file..."
    yi_file = os.path.abspath(os.path.join(directory, "yi_uniques.tsv"))
    yi_unique.to_csv(yi_file, sep="\t", index=True)
    
    # unique isic / cbo
    yo_unique_isic = get_unique(yio_file, "cbo_id", "isic_id")
    
    # print out file
    yo_file = os.path.abspath(os.path.join(directory, "yo.tsv"))
    yo_unique = pd.read_csv(yo_file, sep="\t", converters={"cbo_id":str})
    yo_unique = yo_unique.set_index(["cbo_id"])
    yo_unique["unique_isic"] = yo_unique_isic
    
    print "writing yo file..."
    yo_file = os.path.abspath(os.path.join(directory, "yo_uniques.tsv"))
    yo_unique.to_csv(yo_file, sep="\t", index=True)
    

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
    if year == "all":
        year = range(2002, 2012)
    else:
        year = [int(year)]
    
    for y in year:
        print
        print "Year: {0}".format(y);
        uniques(directory, y)