#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

years=(2003 2004 2005 2006 2007 2008 2009 2010 2011)
tables=(yb ybi ybio ybo yi yio yo)

for year in ${years[*]}
do
  for table in ${tables[*]}
  do
    bash $DIR/import_sql.sh $year $table
  done
done