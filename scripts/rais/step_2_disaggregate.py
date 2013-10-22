# -*- coding: utf-8 -*-
"""
    Clean raw RAIS data and output to CSV
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The script is the first step in adding a new year of RAIS data to the 
    database. The script will output x CSV files that can then be added to
    the database by the add_to_db.py script.
    
    The user needs to specify the path to the file they are looking to use
    as input.
    
    0: Year; 1: Employee_ID; 2: Establishment_ID; 3: Municipality_ID;
    4: BrazilianOcupation_ID; 5: SBCLAS20; 6: CLASCNAE20; 7: WageReceived;
    8: EconomicActivity_ID_ISIC; 9: Average_monthly_wage
"""

''' Import statements '''
import csv, sys, os, argparse, MySQLdb
from collections import defaultdict
from os import environ
import numpy as np

basedir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(basedir, '..'))

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=environ["DATAVIVA_DB_USER"], 
                        passwd=environ["DATAVIVA_DB_PW"], 
                        db=environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def write(tables, year):
    
    vals = ["wage", "num_emp", "num_est"]
    
    '''YO'''
    print
    print ' writing file: yo.tsv'
    csv_file = open(year + '/yo.tsv', 'wb')
    csv_writer = csv.writer(csv_file, delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "cbo_id"]+vals)
    for cbo in tables["yo"].keys():
        csv_writer.writerow([year, cbo, \
            tables["yo"][cbo]['wage'], \
            tables["yo"][cbo]['num_emp'], \
            tables["yo"][cbo]['num_est'] ])
    
    '''YI'''
    print ' writing file: yi.tsv'
    csv_file = open(year + '/yi.tsv', 'wb')
    csv_writer = csv.writer(csv_file, delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "isic_id"]+vals)
    for isic in tables["yi"].keys():
        csv_writer.writerow([year, isic, \
            tables["yi"][isic]['wage'], \
            tables["yi"][isic]['num_emp'], \
            tables["yi"][isic]['num_est'] ])
    
    '''YB'''
    print ' writing file: yb.tsv'
    csv_file = open(year + '/yb.tsv', 'wb')
    csv_writer = csv.writer(csv_file, delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "bra_id"]+vals)
    for bra in tables["yb"].keys():
        csv_writer.writerow([year, bra, \
            tables["yb"][bra]['wage'], \
            tables["yb"][bra]['num_emp'], \
            tables["yb"][bra]['num_est'] ])

    '''YBO'''
    print ' writing file: ybo.tsv'
    csv_file = open(year + '/ybo.tsv', 'wb')
    csv_writer = csv.writer(csv_file, delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "bra_id", "cbo_id"]+vals)
    for bra in tables["ybo"].keys():
        for cbo in tables["ybo"][bra].keys():
            csv_writer.writerow([year, bra, cbo, \
                tables["ybo"][bra][cbo]['wage'], \
                tables["ybo"][bra][cbo]['num_emp'], \
                tables["ybo"][bra][cbo]['num_est'] ])

    '''YBI'''
    print ' writing file: ybi.tsv'
    csv_file = open(year + '/ybi.tsv', 'wb')
    csv_writer = csv.writer(csv_file, delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "bra_id", "isic_id"]+vals)
    for bra in tables["ybi"].keys():
        for isic in tables["ybi"][bra].keys():
            csv_writer.writerow([year, bra, isic, \
                tables["ybi"][bra][isic]['wage'], \
                tables["ybi"][bra][isic]['num_emp'], \
                tables["ybi"][bra][isic]['num_est'] ])
    
    '''YIO'''
    print ' writing file: yio.tsv'
    csv_file = open(year + '/yio.tsv', 'wb')
    csv_writer = csv.writer(csv_file, delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["year", "isic_id", "cbo_id"]+vals)
    for isic in tables["yio"].keys():
        for cbo in tables["yio"][isic].keys():
            csv_writer.writerow([year, isic, cbo, \
                tables["yio"][isic][cbo]['wage'], \
                tables["yio"][isic][cbo]['num_emp'], \
                tables["yio"][isic][cbo]['num_est'] ])


def aggregate(file_path, year):
    tables = {
        "yb": defaultdict(lambda: defaultdict(np.float64)),
        "ybi": defaultdict(lambda: defaultdict(lambda: defaultdict(np.float64))),
        "ybo": defaultdict(lambda: defaultdict(lambda: defaultdict(np.float64))),
        "yi": defaultdict(lambda: defaultdict(np.float64)),
        "yio": defaultdict(lambda: defaultdict(lambda: defaultdict(np.float64))),
        "yo": defaultdict(lambda: defaultdict(np.float64))
    }
    
    '''Open CSV file'''
    csv_reader = csv.reader(open(file_path, 'rU'), delimiter="\t", quotechar='"')
    header = [s.replace('\xef\xbb\xbf', '') for s in csv_reader.next()]
    
    '''Populate the data dictionaries'''
    print 'reading CSV file "' + file_path + '"'
    for i, line in enumerate(csv_reader):
        
        line = dict(zip(header, line))
        
        if i % 100000 == 0:
            sys.stdout.write('\r lines read: ' + str(i) + ' ' * 20)
            sys.stdout.flush() # important
        
        if len(line["bra_id"]) == 8 and len(line["isic_id"]) == 5:
            tables["yo"][line["cbo_id"]]["wage"] += np.float64(line["wage"])
            tables["yo"][line["cbo_id"]]["num_emp"] += np.float64(line["num_emp"])
            tables["yo"][line["cbo_id"]]["num_est"] += np.float64(line["num_est"])
        if len(line["bra_id"]) == 8 and len(line["cbo_id"]) == 4:
            tables["yi"][line["isic_id"]]["wage"] += np.float64(line["wage"])
            tables["yi"][line["isic_id"]]["num_emp"] += np.float64(line["num_emp"])
            tables["yi"][line["isic_id"]]["num_est"] += np.float64(line["num_est"])
        if len(line["isic_id"]) == 5 and len(line["cbo_id"]) == 4:
            tables["yb"][line["bra_id"]]["wage"] += np.float64(line["wage"])
            tables["yb"][line["bra_id"]]["num_emp"] += np.float64(line["num_emp"])
            tables["yb"][line["bra_id"]]["num_est"] += np.float64(line["num_est"])

        if len(line["isic_id"]) == 5:
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["wage"] += np.float64(line["wage"])
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_emp"] += np.float64(line["num_emp"])
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_est"] += np.float64(line["num_est"])
        if len(line["cbo_id"]) == 4:
            tables["ybi"][line["bra_id"]][line["isic_id"]]["wage"] += np.float64(line["wage"])
            tables["ybi"][line["bra_id"]][line["isic_id"]]["num_emp"] += np.float64(line["num_emp"])
            tables["ybi"][line["bra_id"]][line["isic_id"]]["num_est"] += np.float64(line["num_est"])
        if len(line["bra_id"]) == 8:
            tables["yio"][line["isic_id"]][line["cbo_id"]]["wage"] += np.float64(line["wage"])
            tables["yio"][line["isic_id"]][line["cbo_id"]]["num_emp"] += np.float64(line["num_emp"])
            tables["yio"][line["isic_id"]][line["cbo_id"]]["num_est"] += np.float64(line["num_est"])
        
    
    
    write(tables, year)

if __name__ == "__main__":
    
    # Get path of the file from the user
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="full path to CSV file")
    parser.add_argument("-y", "--year", help="year")
    args = parser.parse_args()
    
    file_path = args.file
    if not file_path:
        file_path = raw_input("Full path to CSV file: ")
    
    year = args.year
    if not year:
        year = raw_input("Year: ")
    
    aggregate(file_path, year)