# Introduce
This directory contains experimental data and experimental scripts related to Exp2.

- **construct/** is the experimental data of different methods in the initialization phase. Each subdirectory below it (e.g. effect/6e) represents experimental data for different methods under this query.
- **kernel/** is the experimental data of different methods in the calculating subtree kernel phase.
    - **hashTable.txt** in each subdirectory is the experimental data using hashTable.
    - **suffixTree.txt** in each subdirectory is the experimental data using suffixTree.
    - **draw.py** in each subdirectory is the drawing script.
    - **\*.png** in each subdirectory is the comparison figure of different methods.
- **sql** contains all the SQL statements tested by this experiment.
- **execute.py** is the experiment script used to perform this experiment. It can automatically execute each statement under **sql** and move the output information to the target location.
- **draw.py** is the plotting script used in this experiment. It moves into each subdirectory and calls **draw.py** in that directory.

# Conduct Experiment
This experiment mainly involves two functions `ARENATimeExp4` and `ARENATimeExp4Hash` in file *ARENA_system/back_end/gpdb_src/src/backend/gporca/libgpopt/src/engine/CEngine.cpp*. The former uses the suffix tree to calculate the subtree kernel and the latter uses the hash table to calculate the subtree kernel.

If you want to perform this experiment, you need to choose one of `ARENATimeExp4` and `ARENATimeExp4Hash` to execute, and the other one need to be commented in `CEngine::SamplePlans`. Moreover, in order for **execute.py** to rename the result file correctly, the `move_result` part of the `main` function needs to be commented out as well.