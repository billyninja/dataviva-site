# -*- coding: utf-8 -*-
"""
"""

''' Import statements '''
import csv, sys, os, argparse, MySQLdb, time, bz2
from collections import defaultdict
from os import environ
import pandas as pd
import pandas.io.sql as sql
from ..config import DATA_DIR
from ..helpers import get_file

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=environ["DATAVIVA_DB_USER"], 
                        passwd=environ["DATAVIVA_DB_PW"], 
                        db=environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def get_brazil_rcas(year):
    
    '''Get world values from database'''
    q = "select year, hs_id, rca from comtrade_ypw where year = {0} and "\
        "wld_id = 'sabra'".format(year)
    bra_rcas = sql.read_frame(q, db, index_col=["year", "hs_id"])
    return bra_rcas

def wld_rcas(year, delete_previous_file):
    print year
    
    print "loading yp..."
    yp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yp_pcis_uniques.tsv'))
    yp_file = get_file(yp_file_path)
    yp = pd.read_csv(yp_file, sep="\t", converters={"hs_id": str})
    yp = yp.set_index(["year", "hs_id"])
    
    brazil_rcas = get_brazil_rcas(year)
    
    yp["rca"] = brazil_rcas["rca"]
    
    # print out files
    new_yp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yp_pcis_uniques_rcas.tsv.bz2'))
    print ' writing file:', new_yp_file_path
    yp.to_csv(bz2.BZ2File(new_yp_file_path, 'wb'), sep="\t", index=True)
    
    if delete_previous_file:
        print "deleting previous file"
        os.remove(yp_file.name)

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
    
    wld_rcas(year, delete_previous_file)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;