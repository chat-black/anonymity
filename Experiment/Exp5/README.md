# Introduce
This directory contains experimental data and experimental scripts related to Exp5.

- **1000/** is the experimental data of **random** sampling and the **LAPS** strategy when sampling 1000 plans.
    - **1000/result.txt** is the results of the two sampling algorithms.
    - **1000/Laps.png** is the comparison figure of plan interestingness of two samples.
    - **1000/draw.py** is the drawing script.
- **2000/ 3000/ 4000/** and **5000/** are same as **1000/** except the number of samples are different.
- **LapsCost** records the maximum cost of all plans in imdb_13c.sql. When the sampling plans are different, the maximum cost may also be different, which will affect the normalization of the cost difference. To avoid this problem, we all use the maximum cost of all plans.
- **plan_id.txt** records the ids of all valid plans. When sampling, the algorithm will randomly generate plan ids, and these plans may be invalid, resulting in the number of samples not meeting the expectations. To avoid this problem, we recorded the ids of all valid plans and randomly selected plans only from these plans.
- **sql** contains the SQL statement used in this experiment.
- **execute.py** is the experiment script used to perform this experiment. It can automatically execute each statement under **sql** and move the output information to the target location.
- **draw.py** is the plotting script used in this experiment.

# Conduct Experiment
This experiment involves two functions `ARENALAPSExp` and `ARENALaps` in file *ARENA_system/back_end/gpdb_src/src/backend/gporca/libgpopt/src/engine/CEngine.cpp*. The former is used to conduct this experiment, and the latter is used to execte the LAPS strategy. For convenience, we currently use the **GFP** code to implement the **LAPS** strategy, that is, we generate all the Group Trees, and then select the plan with the same structure as the QEP according to the Group Tree. In fact, we can only generate the Group Tree with the same structure as QEP, and this method will also be faster.

During the experiment, ARENA_system reads the current sql join number and the number of sample from */tmp/gtArg*. Therefore, we can generate results with different numbers of samples by modifying this file. Note that since the plan is chosen randomly in this experiment, repeating the experiment may give different results.