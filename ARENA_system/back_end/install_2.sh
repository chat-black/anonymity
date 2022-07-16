preDir=`pwd`
# check if install.sh is executed
if ! [ -e "/usr/local/gpdb/greenplum_path.sh" ]
then
    echo "Please run install.sh First!"
    exit 1
fi
source /usr/local/gpdb/greenplum_path.sh

cd
mkdir -p gpDatabase/gp_primary
mkdir -p gpDatabase/gpmaster
cd $preDir
cp conf/gpinitsystem_singlenode ~/gpDatabase/
cp conf/hostlist_singlenode ~/gpDatabase/
cd ~/gpDatabase/
hostName=`hostname`
user=$USER
sed -i -e "/MASTER_HOSTNAME=.*/c MASTER_HOSTNAME=$hostName" -e "s/\(\/home\/\)[^\/]*/\1$user/g" gpinitsystem_singlenode
echo $hostName > hostlist_singlenode

echo "test if you can ssh localhost without password..."
if gpssh-exkeys -h localhost > /dev/null
then
    echo "success"
else
    echo "fail!"
    echo "You have to set that you can ssh local host without password..."
    exit 1
fi

echo "test for the existence of configuration files..."
if [ -e "/home/$USER/gpDatabase/gpinitsystem_singlenode" ]
then
    echo "success"
else
    echo "fail!"
    echo "the configuration file doesn't exist!"
    exit 1
fi

gpinitsystem -a -c gpinitsystem_singlenode
createdb tpch  # create a new database
if [ $? -ne 0 ]
then
    echo "init database fail"
    exit 1
fi
cd $preDir


# import tpch data
echo "import tpch data"
cd data/tpch
make clean
make
./dbgen -vf -s 1  # generate data

cwd=`pwd`
sed "s?replace?$cwd?g" load.sql.base > load.sql

ls *.tbl |xargs -I {} sed -i "s/|$//g" {}
psql -d tpch -f ./create.sql
psql -d tpch -f ./load.sql
cd $preDir


# update .bashrc
cd
mkdir -p .local/bin
echo "source /usr/local/gpdb/greenplum_path.sh" >> .bashrc
echo 'export PATH=$PATH:'"$HOME"'/.local/bin' >> .bashrc
cd $preDir
mv conf/recompileGP.sh ~/.local/bin/
mv conf/restartGP.sh ~/.local/bin/
cd ~/.local/bin
chmod a+x recompileGP.sh
chmod a+x restartGP.sh
cd $preDir

echo -e "\n\ninstall Success, Please relogin"