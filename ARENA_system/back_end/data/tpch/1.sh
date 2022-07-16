# make clean
# make
# ./dbgen -vf -s 1
# 
# cwd=`pwd`
# sed "s?replace?$cwd?g" load.sql.base > load.sql

# ls *.tbl |xargs -I {} sed -i "s/|$//g" {}
psql -d tpch -f ./create.sql
psql -d tpch -f ./load.sql
