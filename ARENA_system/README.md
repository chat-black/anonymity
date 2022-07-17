# Introduce
Here is all the source code for the ARENA system.
* back_end/ is the database and TIPS algorithm source code.
    * back_end/conf/ is some configuration files that will be used when installing backend.
    * back_end/data/ is the data used for experiments and tests. Since the IMDB data is too large, we only provide the TPCH data here.
    * back_end/gp-xerces/ is a dependency library when installing the database.
    * back_end/gpdb_src/ is the source code of database(GreenPlum) which contains the ORCA optimizer. Also, the algorithms we proposed (eg, TIPS and GFP) have been integrated into the database source code. The codes of these algorithms and experiments mainly involve two files (back_end/gpdb_src/src/backend/gporca/libgpopt/src/engine/CEngine.cpp) and (back_end/gpdb_src/src/backend/gporca/libgpopt/include/gpopt/search/CTreeMap.h).
    * back_end/install_(1/2).sh is the install script.
* front_end/ is the source code for the front end and web server.

# Install
We experimented and tested on Ubuntu 18.04.6. To ensure problem-free installation and experimentation, we recommend doing it under the same system. The easiest way is to use a virtual machine. In addition, the current front-end js code will request data from the web server at 127.0.0.1, so even if the web server listens to multiple addresses, you can only get data when you access it locally. So, you better install Ubuntu with a GUI.

## Install Backend
We have prepared two installation scripts, back_end/install_1.sh and back_end/install_2.sh, so the installation of the backend is very simple.
* Firstly, `bash install_1.sh` and wait a minute. It will install some necssary libraries and compile the source code of the database.
* Secondly, logout and relogin. This step is necessary. back_end/install_1.sh will modify some default configurations and these modifications will only take effect after relogging.
* Thirdly, `bash install_2.sh` and wait a minute. It will initialize the database and import the tpch data.
* Finally, logout and relogin. back_end/install_2.sh will change .bashrc, so you need to relogin to let it take effect.

Now, you should have successfully completed the installation. You can access the tpch database with the `psql tpch` command. Moreever, we also added the two tools `restartGP.sh` and `recompileGP.sh` to the environment variable PATH. You can use them to restart or recompile the database.

## Install Frontend
The web server is written in Python and the html page has been placed in the correct place. So you just need to install the necessary Python libraries to run the frontend.
* front_end/requirements.txt listed all necessary Python libraries.
* `bash run.sh` to make the web server run. 
* front_end/config.json is a configuration file that controls access to the database and the username used.