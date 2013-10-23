# -*- coding: utf-8 -*-
"""
    Clean raw RAIS data and output to TSV
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The script is the first step in adding a new year of RAIS data to the 
    database. The script will output 1 bzipped TSV file that can then be 
    consumed by step 2 for created the disaggregate tables.
    
    Like many of the other scripts, the user needs to specify the path to the 
    working directory they will be using that will contain the raw file using
    the naming convention 'Rais[year].csv'. The file can be raw text or
    compressed with bzip, gzip or zip.
    
    Raw Text Columns - 
    0: Year; 1: Employee_ID; 2: Establishment_ID; 3: Municipality_ID;
    4: BrazilianOcupation_ID; 5: SBCLAS20; 6: CLASCNAE20; 7: WageReceived;
    8: EconomicActivity_ID_ISIC; 9: Average_monthly_wage
"""

''' Import statements '''
import csv, sys, os, argparse, MySQLdb, time, bz2, gzip, zipfile
from collections import defaultdict
from os import environ
from decimal import Decimal, ROUND_HALF_UP
from ..config import DATA_DIR
from ..helpers import d

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=environ["DATAVIVA_DB_USER"], 
                        passwd=environ["DATAVIVA_DB_PW"], 
                        db=environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def cbo_format(cbo_code, lookup):
    # take off last 2 digits
    cbo = cbo_code[:-2]
    if cbo in ['', 'NAO DESL A', 'IGNORA', '-94201', '0000']:
        return "xxxx"
    elif cbo[0] == '0':
        return "xxxx"
    else:
        return lookup[cbo]

def isic_format(isic_code, lookup):
    isic = isic_code.zfill(4)
    if isic in ['0000', '-94201', '0001']:
        return "x0000"
    else:
        return lookup[isic]

def munic_format(munic, lookup):
    try:
        return lookup[munic]
    except:
        print munic
        sys.exit()

def wage_format(wage):
    # convert commas to dots
    wage = wage.replace(",", ".")
    # cast to float
    return float(wage)

def get_lookup(type):
    if type == "bra":
        cursor.execute("select id_ibge, id from attrs_bra where length(id)=8")
        return {str(r[0])[:-1]:r[1] for r in cursor.fetchall()}
    elif type == "isic":
        cursor.execute("select id from attrs_isic")
        return {r[0][1:]:r[0] for r in cursor.fetchall()}
    elif type == "cbo":
        cursor.execute("select id from attrs_cbo")
        return {r[0]:r[0] for r in cursor.fetchall()}
    elif type == "pr":
        cursor.execute("select bra_id, pr_id from attrs_bra_pr")
        return {r[0]:r[1] for r in cursor.fetchall()}

def add(ybio, munic, isic, occ, wage, emp, est):
    ybio[munic][isic][occ]["wage"] += float(wage)
    ybio[munic][isic][occ]["num_emp"] += 1
    ybio[munic][isic][occ]["wage_avg"] = ybio[munic][isic][occ]["wage"] / ybio[munic][isic][occ]["num_emp"]
    
    try:
        ybio[munic][isic][occ]["num_est"].add(est)
    except AttributeError:
        ybio[munic][isic][occ]["num_est"] = set([est])
    
    ybio[munic][isic][occ]["num_emp_est"] = ybio[munic][isic][occ]["num_emp"] / len(ybio[munic][isic][occ]["num_est"])
    
    return ybio

def get_file(year):
    extensions = [
        {'ext':'.csv.bz2', 'io':bz2.BZ2File},
        {'ext':'.csv.gz', 'io':gzip.open},
        {'ext':'.csv.zip', 'io':zipfile.ZipFile},
        {'ext':'.csv', 'io':open}
    ]
    for e in extensions:
        file_name = "Rais{0}{1}".format(year, e["ext"])
        file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', file_name))
        if os.path.exists(file_path):
            file = e["io"](file_path)
            if e["ext"] == '.csv.zip':
                file = zipfile.ZipFile.open(file, "Rais{0}.csv".format(year))
            print "Reading from file", file_path
            return file
    print "ERROR: unable to find file named Rais{0}.csv[.zip, .bz2, .gz] " \
            "in directory specified.".format(year)
    sys.exit()

def clean(year):
    '''Initialize our data dictionaries'''
    ybio = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(float))))
    
    lookup = {"munic": get_lookup("bra"), "isic": get_lookup("isic"), "occ": get_lookup("cbo"), "pr": get_lookup("pr")}
    
    var_names = {"year":["Year", int], "est":"Establishment_ID", \
                    "emp":"Employee_ID", "munic": ["Municipality_ID", munic_format], \
                    "occ": ["BrazilianOcupation_ID", cbo_format], \
                    "isic": ["EconomicAtivity_ID_ISIC", isic_format], \
                    "wage":["AverageMonthlyWage", wage_format]}
    
    '''Open CSV file'''
    file = get_file(year)
    csv_reader = csv.reader(file, delimiter=",", quotechar='"')
    header = [s.replace('\xef\xbb\xbf', '') for s in csv_reader.next()]
    
    errors_dict = defaultdict(set)
    
    '''Populate the data dictionaries'''
    for i, line in enumerate(csv_reader):
        
        line = dict(zip(header, line))
        
        if i % 100000 == 0 and i != 0:
            sys.stdout.write('\r lines read: ' + str(i) + ' ' * 20)
            sys.stdout.flush() # important
        
        data = var_names.copy()
        errors = False
        
        for var, var_name in data.items():
            formatter = None
            if isinstance(var_name, list):
                var_name, formatter = var_name
            
            try:
                data[var] = line[var_name].strip()
            except KeyError:
                print
                # print "Error reading year on line {0}".format(i+1)
                if var_name == "EconomicAtivity_ID_ISIC":
                    new_col = "EconmicAtivity_ID_ISIC"
                else:
                    new_col = raw_input('Could not find value for "{0}" column. '\
                                'Use different column name? : ' \
                                .format(var_name))
                if isinstance(var_names[var], list):
                    var_names[var][0] = new_col
                else:
                    var_names[var] = new_col
                try:
                    data[var] = line[new_col].strip()
                except KeyError:
                    errors = True
                    continue
            
            # run proper formatting
            if formatter:
                try:
                    if var in lookup:
                        data[var] = formatter(data[var], lookup[var])
                    else:
                        data[var] = formatter(data[var])
                except:
                    # print data[var]
                    print "Error reading {0} ID on line {1}".format(var, i+1)
                    errors_dict[var].add(data[var])
                    errors = True
                    continue
        
        if errors:
            continue
        
        data["est"] = data["munic"]+data["isic"]+data["occ"]+data["est"]
        ybio = add(ybio, data["munic"], data["isic"], data["occ"], data["wage"], data["emp"], data["est"])

        # cbo 1digit
        ybio = add(ybio, data["munic"], data["isic"], data["occ"][:1], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"], data["isic"], data["occ"][:2], data["wage"], data["emp"], data["est"])
        
        # isic 1digit
        ybio = add(ybio, data["munic"], data["isic"][:3], data["occ"][:1], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"], data["isic"][:3], data["occ"][:2], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"], data["isic"][:3], data["occ"], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"], data["isic"][:1], data["occ"][:1], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"], data["isic"][:1], data["occ"][:2], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"], data["isic"][:1], data["occ"], data["wage"], data["emp"], data["est"])
        
        # bra 4digit
        ybio = add(ybio, data["munic"][:4], data["isic"][:1], data["occ"][:1], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:4], data["isic"][:3], data["occ"][:1], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:4], data["isic"][:1], data["occ"][:2], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:4], data["isic"][:3], data["occ"][:2], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:4], data["isic"][:1], data["occ"], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:4], data["isic"][:3], data["occ"], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:4], data["isic"], data["occ"][:1], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:4], data["isic"], data["occ"][:2], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:4], data["isic"], data["occ"], data["wage"], data["emp"], data["est"])
        
        # bra 2digit
        ybio = add(ybio, data["munic"][:2], data["isic"][:1], data["occ"][:1], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:2], data["isic"][:3], data["occ"][:1], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:2], data["isic"][:1], data["occ"][:2], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:2], data["isic"][:3], data["occ"][:2], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:2], data["isic"][:1], data["occ"], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:2], data["isic"][:3], data["occ"], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:2], data["isic"], data["occ"][:1], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:2], data["isic"], data["occ"][:2], data["wage"], data["emp"], data["est"])
        ybio = add(ybio, data["munic"][:2], data["isic"], data["occ"], data["wage"], data["emp"], data["est"])
        
        if data["munic"] in lookup["pr"]:
            ybio = add(ybio, lookup["pr"][data["munic"]], data["isic"][:1], data["occ"][:1], data["wage"], data["emp"], data["est"])
            ybio = add(ybio, lookup["pr"][data["munic"]], data["isic"][:3], data["occ"][:1], data["wage"], data["emp"], data["est"])
            ybio = add(ybio, lookup["pr"][data["munic"]], data["isic"][:1], data["occ"][:2], data["wage"], data["emp"], data["est"])
            ybio = add(ybio, lookup["pr"][data["munic"]], data["isic"][:3], data["occ"][:2], data["wage"], data["emp"], data["est"])
            ybio = add(ybio, lookup["pr"][data["munic"]], data["isic"][:1], data["occ"], data["wage"], data["emp"], data["est"])
            ybio = add(ybio, lookup["pr"][data["munic"]], data["isic"][:3], data["occ"], data["wage"], data["emp"], data["est"])
            ybio = add(ybio, lookup["pr"][data["munic"]], data["isic"], data["occ"][:1], data["wage"], data["emp"], data["est"])
            ybio = add(ybio, lookup["pr"][data["munic"]], data["isic"], data["occ"][:2], data["wage"], data["emp"], data["est"])
            ybio = add(ybio, lookup["pr"][data["munic"]], data["isic"], data["occ"], data["wage"], data["emp"], data["est"])
        
        # if i == 10000:
        #     break
    
    print errors_dict
    
    columns = {"y":"year", "b":"bra_id", "i":"isic_id", "o":"cbo_id"}
    
    print
    print "finished reading file, writing output..."
    
    new_dir = os.path.abspath(os.path.join(DATA_DIR, 'rais', year))
    if not os.path.exists(new_dir):
        os.makedirs(new_dir)
    
    new_file = os.path.abspath(os.path.join(new_dir, "ybio.tsv.bz2"))
    print ' writing file: ', new_file
    
    '''Create header for CSV file'''
    header = ["year", "bra_id", "isic_id", "cbo_id", "wage", "num_emp", "num_est", "wage_avg", "num_emp_est"]
    
    '''Export to files'''
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(header)
    
    for bra in ybio.keys():
        for isic in ybio[bra].keys():
            for cbo in ybio[bra][isic].keys():
                csv_writer.writerow([year, bra, isic, cbo, \
                    d(ybio[bra][isic][cbo]['wage']), \
                    int(ybio[bra][isic][cbo]['num_emp']), \
                    len(ybio[bra][isic][cbo]['num_est']), \
                    d(ybio[bra][isic][cbo]['wage_avg']), \
                    ybio[bra][isic][cbo]['num_emp_est'] ])

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
    
    clean(year)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;