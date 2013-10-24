# -*- coding: utf-8 -*-
"""
"""

''' Import statements '''
import csv, sys, os, argparse, MySQLdb, bz2, time
from collections import defaultdict
from os import environ
from decimal import Decimal, ROUND_HALF_UP
from ..config import DATA_DIR
from ..helpers import d, get_file

def write(tables, year):
    
    vals = ["val_usd"]
    directory = os.path.abspath(os.path.join(DATA_DIR, 'secex'))
    
    '''YW'''
    print
    new_file = os.path.abspath(os.path.join(directory, year, "yw.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "cbo_id"]+vals)
    for wld in tables["yw"].keys():
        csv_writer.writerow([year, wld, d(tables["yw"][wld]['val_usd'])])
    
    '''YP'''
    new_file = os.path.abspath(os.path.join(directory, year, "yp.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "hs_id"]+vals)
    for hs in tables["yp"].keys():
        csv_writer.writerow([year, hs, d(tables["yp"][hs]['val_usd'])])
    
    '''YB'''
    new_file = os.path.abspath(os.path.join(directory, year, "yb.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "bra_id"]+vals)
    for bra in tables["yb"].keys():
        csv_writer.writerow([year, bra, d(tables["yb"][bra]['val_usd']) ])

    '''YBW'''
    new_file = os.path.abspath(os.path.join(directory, year, "ybw.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "bra_id", "wld_id"]+vals)
    for bra in tables["ybw"].keys():
        for wld in tables["ybw"][bra].keys():
            csv_writer.writerow([year, bra, wld, d(tables["ybw"][bra][wld]['val_usd']) ])

    '''YBP'''
    new_file = os.path.abspath(os.path.join(directory, year, "ybp.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "bra_id", "hs_id"]+vals)
    for bra in tables["ybp"].keys():
        for hs in tables["ybp"][bra].keys():
            csv_writer.writerow([year, bra, hs, d(tables["ybp"][bra][hs]['val_usd']) ])
    
    '''YPW'''
    new_file = os.path.abspath(os.path.join(directory, year, "ypw.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "hs_id", "wld_id"]+vals)
    for hs in tables["ypw"].keys():
        for wld in tables["ypw"][hs].keys():
            csv_writer.writerow([year, hs, wld, \
                d(tables["ypw"][hs][wld]['val_usd']) ])

def disaggregate(year):
    tables = {
        "yb": defaultdict(lambda: defaultdict(float)),
        "yp": defaultdict(lambda: defaultdict(float)),
        "yw": defaultdict(lambda: defaultdict(float)),
        "ybp": defaultdict(lambda: defaultdict(lambda: defaultdict(float))),
        "ybw": defaultdict(lambda: defaultdict(lambda: defaultdict(float))),
        "ypw": defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    }
    
    '''Open CSV file'''
    ybpw_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'ybpw.tsv'))
    ybpw_file = get_file(ybpw_file_path)
    
    csv_reader = csv.reader(ybpw_file, delimiter="\t", quotechar='"')
    header = [s.replace('\xef\xbb\xbf', '') for s in csv_reader.next()]
    
    '''Populate the data dictionaries'''
    for i, line in enumerate(csv_reader):
        
        line = dict(zip(header, line))
        
        if i % 100000 == 0:
            sys.stdout.write('\r lines read: ' + str(i) + ' ' * 20)
            sys.stdout.flush() # important
        
        if len(line["bra_id"]) == 8 and len(line["hs_id"]) == 6:
            tables["yw"][line["wld_id"]]["val_usd"] += float(line["val_usd"])
        if len(line["bra_id"]) == 8 and len(line["wld_id"]) == 5:
            tables["yp"][line["hs_id"]]["val_usd"] += float(line["val_usd"])
        if len(line["hs_id"]) == 6 and len(line["wld_id"]) == 5:
            tables["yb"][line["bra_id"]]["val_usd"] += float(line["val_usd"])

        if len(line["hs_id"]) == 6:
            tables["ybw"][line["bra_id"]][line["wld_id"]]["val_usd"] += float(line["val_usd"])
        if len(line["wld_id"]) == 5:
            tables["ybp"][line["bra_id"]][line["hs_id"]]["val_usd"] += float(line["val_usd"])
        if len(line["bra_id"]) == 8:
            tables["ypw"][line["hs_id"]][line["wld_id"]]["val_usd"] += float(line["val_usd"])
    
    write(tables, year)

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
    
    disaggregate(year)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;