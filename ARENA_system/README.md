# Introduce
This directory contains the source code of the ARENA system.
- **back_end/** contains the source code of database and TPCH data.
    - **back_end/conf/** is some configuration files that will be used when installing backend.
    - **back_end/data/** is the data used for experiments and tests. Since the IMDB data is too large, we only provide the TPCH data here. The IMDB data can be downloaded from <http://homepages.cwi.nl/~boncz/job/imdb.tgz>.
    - **back_end/gp-xerces/** is a dependency library when installing the database.
    - **back_end/gpdb_src/** is the source code of database(GreenPlum) which contains the ORCA optimizer. Also, the algorithms we proposed (e.g. **TIPS** and **GFP**) have been integrated into the database source code. The codes of these algorithms and experiments mainly involve two files (*back_end/gpdb_src/src/backend/gporca/libgpopt/src/engine/CEngine.cpp*) and (*back_end/gpdb_src/src/backend/gporca/libgpopt/include/gpopt/search/CTreeMap.h*). The functions corresponding to each algorithm are as follows:
        - `ARENA_TIPS` is the wrapper function for **TIPS** algorithm. It will call the **I-TIPS** algorithm (function `I_TIPS`) or the **B-TIPS-Heap** algorithm (function `B_TIPS_Heap`) as needed. All of them are in *CEngine.cpp*.
        - `ARENAGFPFilter` implements the **GFP** or **GFP&Cost** filter strategy which is in *CEngine.cpp*. The **Group Forest** is built in *CTreeMap.h*. And the macro `ARENA_COSTFT` defined in *CTreeMap.h* is used to control whether the **GFP** strategy or the **GFP&Cost** strategy is used.
        - `ARENALaps` implemets the **LAPS** strategy and it is in *CEngine.cpp*. For convenience, we currently use the **Group Forest** in the **GFP** strategy to find the plans which have the same structure as QEP. But this is not necessary, we will improve it later.
        - `CEngine::SamplePlans` in *CEngine.cpp* is the function that is used to find all valid alternative plans. 
    - **back_end/install_(1/2).sh** is the install script.
- **front_end/** is the source code for the front end and web server.

# Install
We experimented and tested on Ubuntu 18.04.6. To ensure problem-free installation and experimentation, we recommend doing it under the same system. The easiest way is to use a virtual machine. In addition, the current front-end js code will request data from the web server at 127.0.0.1, so even if the web server listens to multiple addresses, you can only get data when you access it locally. So, you better install Ubuntu with a GUI.

## Install Backend
We have prepared two installation scripts, **back_end/install_1.sh** and **back_end/install_2.sh**, so the installation of the backend is very simple.
* Firstly, `bash install_1.sh` and wait a minute. It will install some necssary libraries and compile the source code of the database.
* Secondly, logout and relogin. This step is necessary. back_end/install_1.sh will modify some default configurations and these modifications will only take effect after relogging.
* Thirdly, `bash install_2.sh` and wait a minute. It will initialize the database and import the tpch data.
* Finally, logout and relogin. back_end/install_2.sh will change .bashrc, so you need to relogin to let it take effect.

Now, you should have successfully completed the installation. You can access the tpch database with the `psql tpch` command. Moreever, we also added the two tools `restartGP.sh` and `recompileGP.sh` to the environment variable PATH. You can use them to restart or recompile the database.

## Install Frontend
The web server is written in Python3.6 and the html page has been placed in the correct place. So you just need to install the necessary Python libraries to run the frontend.
* front_end/requirements.txt listed all necessary Python libraries.
* `bash front_end/run.sh` to make the web server run. 
* front_end/config.json is a configuration file that controls which database the web server accesses and what username to use.

## Run
After successfully installing the backend and frontend, you can run the ARENA system.
* First, make sure the database is running. If you are not sure if it is running, just execute `restartGP.sh` to restart.
* Then go to the **front_end/** directory and execute `bash run.sh` to start the web server.
* Finally you can access the ARENA system by visiting 127.0.0.1:5000 through your browser. 