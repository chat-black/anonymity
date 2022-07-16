#!/bin/bash

function checkState() {
  if [ $? -ne 0 ]; then  # fail
    echo $1
    exit 1
  else
    return
  fi
}

# Download the necessary packages
sudo apt-get update
sudo apt-get install -y \
  python \
  bison \
  ccache \
  cmake \
  curl \
  flex \
  git-core \
  gcc \
  g++ \
  inetutils-ping \
  libapr1-dev \
  libbz2-dev \
  libcurl4-gnutls-dev \
  libevent-dev \
  libpam-dev \
  libperl-dev \
  libreadline-dev \
  libssl-dev \
  libxml2-dev \
  libyaml-dev \
  libzstd-dev \
  locales \
  net-tools \
  ninja-build \
  openssh-client \
  openssh-server \
  openssl \
  python-dev \
  python-pip \
  python-psutil \
  python-yaml \
  python3-pip \
  zlib1g-dev
checkState "I can't install necessary package"


# install gp-xerces
preDir=`pwd`
cd gp-xerces/build/
../configure --prefix=/usr/local
make
sudo make install
checkState "install gp-xerces failed"
cd $preDir

# install gpdb
cd gpdb_src/
sourceDir=`pwd`
cd $preDir
cd conf
sed -e "s?databaseSource?$sourceDir?g" -e "s?username?$USER?g" recompileGP.sh.base > recompileGP.sh
sed -e "s?databaseSource?$sourceDir?g" -e "s?username?$USER?g" restartGP.sh.base > restartGP.sh
cd $sourceDir
./configure --with-perl --with-python --with-libxml CC=gcc CFLAGS=-O0 CPPFLAGS='-O0 -std=c++11'
make
sudo make install
checkState "install orca failed"
cd $preDir

# move xerces library to correct directory
sudo cp /usr/local/lib/libxerces-c-3.1.so /usr/local/gpdb/lib/
sudo cp /usr/local/lib/libxerces-c.a /usr/local/gpdb/lib/
sudo cp /usr/local/lib/libxerces-c.la /usr/local/gpdb/lib/

# set up local password-free login
cd
echo -e "\n\n\n\n\n" > .ARENA_temp
ssh-keygen < .ARENA_temp
echo -e "\n\n******************** Now, we are setting up ssh to localhost without password, which is necessary. ********************"
ssh-copy-id -i ~/.ssh/id_rsa.pub $USER@localhost
rm .ARENA_temp
cd $preDir

# edit /etc/security/limits.conf to change the max open files, you need to relogin
addLimits="* hard nofile 65535\n* soft nofile 65535"
if ! grep "hard nofile 65535" /etc/security/limits.conf> /dev/null; then
    echo -e "$addLimits" | sudo tee -a /etc/security/limits.conf > /dev/null
fi

echo -e "\n\n******************** Please relogin and execute install_2.sh ********************"

