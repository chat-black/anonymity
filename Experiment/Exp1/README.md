# Introduce
This directory contains experimental data and experimental scripts related to Exp1.

- **effect/** is the experimental data on the effects (plan interestingness) of different algorithms. Each subdirectory below it (e.g. effect/imdb/1a) represents experimental data for different algorithms under this query.
- **efficiency/** is the experimental data on the efficiency (run time) of different algorithms.
    - **tips.txt** in each subdirectory is the result of B-TIPS-Heap.
    - **tips_b.txt** in each subdirectory is the result of B-TIPS-Basic.
    - **random.txt** in each subdirectory is the result of Random.
    - **cost.txt** in each subdirectory is the result of Cost.
    - **draw.py** in each subdirectory is the drawing script.
    - **efficiency.png** in each subdirectory is the comparison figure of different algorithms.
- **sql** contains all the SQL statements tested by this experiment.
- **execute.py** is the experiment script used to perform this experiment. It can automatically execute each statement under **sql** and move the output information to the target location.
- **draw.py** is the plotting script used in this experiment. It moves into each subdirectory and calls **draw.py** in that directory.

# Conduct Experiment
This experiment mainly involves three functions `ARENATimeExp3`, `ARENATimeExp3Random`, and `ARENATimeExp3Cost` in file *ARENA_system/back_end/gpdb_src/src/backend/gporca/libgpopt/src/engine/CEngine.cpp*. They represent three different algorithms, **B-TIPS**, **Random** and **Cost**, respectively. By adjusting part of the code, `ARENATimeExp3` can implement **B-TIPS-Basic** or **B-TIPS-Heap**.

If you want to perform this experiment, you need to choose one of `ARENATimeExp3`, `ARENATimeExp3Random`, and `ARENATimeExp3Cost` to execute, and the other two need to be commented in `CEngine::SamplePlans`. Moreover, in order for **execute.py** to rename the result file correctly, the `move_result` part of the `main` function needs to be commented out as well.