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

def growth(file_current, file_prev, file_prev_5, indexes):
    index_lookup = {"b":"bra_id", "i":"isic_id", "o":"cbo_id"}
    index_cols = [index_lookup[i] for i in indexes if i != "y"]
    converters = {"cbo_id": str} if "o" in indexes else None
    
    print "loading current year"
    current = pd.read_csv(file_current, sep="\t", converters=converters)
    current = current.set_index(index_cols)
    
    print "loading previous year"
    prev = pd.read_csv(file_prev, sep="\t", converters=converters)
    prev = prev.set_index(index_cols)
    
    print "calculating 1 year wage growth value"
    s = time.time()
    current["wage_growth_val"] = current["wage"] - prev["wage"]
    print (time.time() - s) / 60
    
    print "calculating 1 year wage growth rate"
    s = time.time()
    current["wage_growth_rate"] = (current["wage"] / prev["wage"]) - 1
    print (time.time() - s) / 60

    print "calculating 1 year num_emp growth value"
    s = time.time()
    current["num_emp_growth_val"] = current["num_emp"] - prev["num_emp"]
    print (time.time() - s) / 60

    print "calculating 1 year num_emp growth rate"
    s = time.time()
    current["num_emp_growth_rate"] = (current["num_emp"] / prev["num_emp"]) - 1
    print (time.time() - s) / 60
    
    if file_prev_5:
        print "loading file for 5 years ago"
        prev_5 = pd.read_csv(file_prev_5, sep="\t", converters=converters)
        prev_5 = prev_5.set_index(index_cols)
        
        print "calculating 5 year wage growth value"
        current["wage_growth_val_5"] = current["wage"] - prev_5["wage"]
    
        print "calculating 5 year wage growth rate"
        current["wage_growth_rate_5"] = (current["wage"] / prev_5["wage"]) ** (1.0/5.0) - 1
        
        print "calculating 5 year num_emp growth value"
        current["num_emp_growth_val_5"] = current["num_emp"] - prev_5["num_emp"]
    
        print "calculating 5 year num_emp growth rate"
        current["num_emp_growth_rate_5"] = (current["num_emp"] / prev_5["num_emp"]) ** (1.0/5.0) - 1
    
    # print out file
    new_file_path = file_current.split("/")
    new_file_path[-1] = new_file_path[-1].split(".")[0] + "_growth.tsv"
    new_file_path = "/".join(new_file_path)
    print "writing {0}...".format(new_file_path)
    
    current.to_csv(new_file_path, sep="\t", index=True)
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--indexes", help="index columns on data i.e. ybi, ybio, ybo etc.")
    parser.add_argument("-f", "--file_current", help="full path to CSV file")
    parser.add_argument("-p", "--file_prev", help="full path to CSV file ")
    parser.add_argument("-p5", "--file_prev_5", help="full path to CSV file")
    args = parser.parse_args()

    indexes = args.indexes
    if not indexes:
        indexes = raw_input("What are the index for these files: ")    
    file_current = args.file_current
    if not file_current:
        file_current = raw_input("Full path to current CSV file: ")
    file_prev = args.file_prev
    if not file_prev:
        file_prev = raw_input("Full path to previous year's CSV file: ")
    
    growth(file_current, file_prev, args.file_prev_5, indexes)