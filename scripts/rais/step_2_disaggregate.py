# -*- coding: utf-8 -*-
"""
    Disaggregate YBIO to subsequent aggregate table
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The script is the second step in adding a new year of RAIS data to the 
    database. The script will output 6 bzipped TSV files that can then be 
    used throughout the rest of the steps.
    
    Like many of the other scripts, the user needs to specify the path to the 
    working directory they will be using that will contain the folder with
    the year they are looking to use including a ybio.tsv[.bz2] file. The year
    also needs to be specified.
    
"""

''' Import statements '''
import csv, sys, os, argparse, MySQLdb, bz2, gzip, zipfile, time
from collections import defaultdict
from os import environ
from decimal import Decimal, ROUND_HALF_UP
from ..config import DATA_DIR
from ..helpers import d

def write(tables, directory, year):
    
    vals = ["wage", "num_emp", "num_est", "wage_avg", "num_emp_est"]
    
    '''YO'''
    print
    new_file = os.path.abspath(os.path.join(directory, year, "yo.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "cbo_id"]+vals)
    for cbo in tables["yo"].keys():
        csv_writer.writerow([year, cbo, \
            d(tables["yo"][cbo]['wage']), \
            int(tables["yo"][cbo]['num_emp']), \
            int(tables["yo"][cbo]['num_est']), \
            d(tables["yo"][cbo]['wage_avg']), \
            tables["yo"][cbo]['num_emp_est'] ])
    
    '''YI'''
    new_file = os.path.abspath(os.path.join(directory, year, "yi.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "isic_id"]+vals)
    for isic in tables["yi"].keys():
        csv_writer.writerow([year, isic, \
            d(tables["yi"][isic]['wage']), \
            int(tables["yi"][isic]['num_emp']), \
            int(tables["yi"][isic]['num_est']), \
            d(tables["yi"][isic]['wage_avg']), \
            tables["yi"][isic]['num_emp_est'] ])
    
    '''YB'''
    new_file = os.path.abspath(os.path.join(directory, year, "yb.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "bra_id"]+vals)
    for bra in tables["yb"].keys():
        csv_writer.writerow([year, bra, \
            d(tables["yb"][bra]['wage']), \
            int(tables["yb"][bra]['num_emp']), \
            int(tables["yb"][bra]['num_est']), \
            d(tables["yb"][bra]['wage_avg']), \
            tables["yb"][bra]['num_emp_est'] ])

    '''YBO'''
    new_file = os.path.abspath(os.path.join(directory, year, "ybo.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "bra_id", "cbo_id"]+vals)
    for bra in tables["ybo"].keys():
        for cbo in tables["ybo"][bra].keys():
            csv_writer.writerow([year, bra, cbo, \
                d(tables["ybo"][bra][cbo]['wage']), \
                int(tables["ybo"][bra][cbo]['num_emp']), \
                int(tables["ybo"][bra][cbo]['num_est']), \
                d(tables["ybo"][bra][cbo]['wage_avg']), \
                tables["ybo"][bra][cbo]['num_emp_est'] ])

    '''YBI'''
    new_file = os.path.abspath(os.path.join(directory, year, "ybi.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "bra_id", "isic_id"]+vals)
    for bra in tables["ybi"].keys():
        for isic in tables["ybi"][bra].keys():
            csv_writer.writerow([year, bra, isic, \
                d(tables["ybi"][bra][isic]['wage']), \
                int(tables["ybi"][bra][isic]['num_emp']), \
                int(tables["ybi"][bra][isic]['num_est']), \
                d(tables["ybi"][bra][isic]['wage_avg']), \
                tables["ybi"][bra][isic]['num_emp_est'] ])
    
    '''YIO'''
    new_file = os.path.abspath(os.path.join(directory, year, "yio.tsv.bz2"))
    print ' writing file:', new_file
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "isic_id", "cbo_id"]+vals)
    for isic in tables["yio"].keys():
        for cbo in tables["yio"][isic].keys():
            csv_writer.writerow([year, isic, cbo, \
                d(tables["yio"][isic][cbo]['wage']), \
                int(tables["yio"][isic][cbo]['num_emp']), \
                int(tables["yio"][isic][cbo]['num_est']) ])

def get_file(directory, year):
    extensions = [
        {'ext':'.tsv.bz2', 'io':bz2.BZ2File},
        {'ext':'.tsv.gz', 'io':gzip.open},
        {'ext':'.tsv.zip', 'io':zipfile.ZipFile},
        {'ext':'.tsv', 'io':open}
    ]
    for e in extensions:
        file_name = "ybio{0}".format(e["ext"])
        file_path = os.path.abspath(os.path.join(directory, year, file_name))
        if os.path.exists(file_path):
            file = e["io"](file_path)
            if e["ext"] == '.tsv.zip':
                file = zipfile.ZipFile.open(file, "ybio.tsv")
            print "Reading from file", file_path
            return file
    print "ERROR: unable to find file named ybio.tsv[.zip, .bz2, .gz] " \
            "in directory specified."
    sys.exit()


def disaggregate(year):
    tables = {
        "yb": defaultdict(lambda: defaultdict(float)),
        "ybi": defaultdict(lambda: defaultdict(lambda: defaultdict(float))),
        "ybo": defaultdict(lambda: defaultdict(lambda: defaultdict(float))),
        "yi": defaultdict(lambda: defaultdict(float)),
        "yio": defaultdict(lambda: defaultdict(lambda: defaultdict(float))),
        "yo": defaultdict(lambda: defaultdict(float))
    }
    directory = os.path.abspath(os.path.join(DATA_DIR, "rais"))
    
    '''Open CSV file'''
    file = get_file(directory, year)
    csv_reader = csv.reader(file, delimiter="\t", quotechar='"')
    header = [s.replace('\xef\xbb\xbf', '') for s in csv_reader.next()]
    
    '''Populate the data dictionaries'''
    for i, line in enumerate(csv_reader):
        
        line = dict(zip(header, line))
        
        if i % 100000 == 0:
            sys.stdout.write('\r lines read: ' + str(i) + ' ' * 20)
            sys.stdout.flush() # important
        
        if len(line["bra_id"]) == 8 and len(line["isic_id"]) == 5:
            tables["yo"][line["cbo_id"]]["wage"] += float(line["wage"])
            tables["yo"][line["cbo_id"]]["num_emp"] += int(line["num_emp"])
            tables["yo"][line["cbo_id"]]["num_est"] += int(line["num_est"])
            tables["yo"][line["cbo_id"]]["wage_avg"] = tables["yo"][line["cbo_id"]]["wage"] / tables["yo"][line["cbo_id"]]["num_emp"]
            tables["yo"][line["cbo_id"]]["num_emp_est"] = float(tables["yo"][line["cbo_id"]]["num_emp"]) / tables["yo"][line["cbo_id"]]["num_est"]
        if len(line["bra_id"]) == 8 and len(line["cbo_id"]) == 4:
            tables["yi"][line["isic_id"]]["wage"] += float(line["wage"])
            tables["yi"][line["isic_id"]]["num_emp"] += int(line["num_emp"])
            tables["yi"][line["isic_id"]]["num_est"] += int(line["num_est"])
            tables["yi"][line["isic_id"]]["wage_avg"] = tables["yi"][line["isic_id"]]["wage"] / tables["yi"][line["isic_id"]]["num_emp"]
            tables["yi"][line["isic_id"]]["num_emp_est"] = float(tables["yi"][line["isic_id"]]["num_emp"]) / tables["yi"][line["isic_id"]]["num_est"]
        if len(line["isic_id"]) == 5 and len(line["cbo_id"]) == 4:
            tables["yb"][line["bra_id"]]["wage"] += float(line["wage"])
            tables["yb"][line["bra_id"]]["num_emp"] += int(line["num_emp"])
            tables["yb"][line["bra_id"]]["num_est"] += int(line["num_est"])
            tables["yb"][line["bra_id"]]["wage_avg"] = tables["yb"][line["bra_id"]]["wage"] / tables["yb"][line["bra_id"]]["num_emp"]
            tables["yb"][line["bra_id"]]["num_emp_est"] = float(tables["yb"][line["bra_id"]]["num_emp"]) / tables["yb"][line["bra_id"]]["num_est"]

        if len(line["isic_id"]) == 5:
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["wage"] += float(line["wage"])
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_emp"] += int(line["num_emp"])
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_est"] += int(line["num_est"])
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["wage_avg"] = tables["ybo"][line["bra_id"]][line["cbo_id"]]["wage"] / tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_emp"]
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_emp_est"] = float(tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_emp"]) / tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_est"]
        if len(line["cbo_id"]) == 4:
            tables["ybi"][line["bra_id"]][line["isic_id"]]["wage"] += float(line["wage"])
            tables["ybi"][line["bra_id"]][line["isic_id"]]["num_emp"] += int(line["num_emp"])
            tables["ybi"][line["bra_id"]][line["isic_id"]]["num_est"] += int(line["num_est"])
            tables["ybi"][line["bra_id"]][line["isic_id"]]["wage_avg"] = tables["ybi"][line["bra_id"]][line["isic_id"]]["wage"] / tables["ybi"][line["bra_id"]][line["isic_id"]]["num_emp"]
            tables["ybi"][line["bra_id"]][line["isic_id"]]["num_emp_est"] = float(tables["ybi"][line["bra_id"]][line["isic_id"]]["num_emp"]) / tables["ybi"][line["bra_id"]][line["isic_id"]]["num_est"]
        if len(line["bra_id"]) == 8:
            tables["yio"][line["isic_id"]][line["cbo_id"]]["wage"] += float(line["wage"])
            tables["yio"][line["isic_id"]][line["cbo_id"]]["num_emp"] += int(line["num_emp"])
            tables["yio"][line["isic_id"]][line["cbo_id"]]["num_est"] += int(line["num_est"])
            tables["yio"][line["isic_id"]][line["cbo_id"]]["wage_avg"] = tables["yio"][line["isic_id"]][line["cbo_id"]]["wage"] / tables["yio"][line["isic_id"]][line["cbo_id"]]["num_emp"]
            tables["yio"][line["isic_id"]][line["cbo_id"]]["num_emp_est"] = float(tables["yio"][line["isic_id"]][line["cbo_id"]]["num_emp"]) / tables["yio"][line["isic_id"]][line["cbo_id"]]["num_est"]
    
    write(tables, directory, year)

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