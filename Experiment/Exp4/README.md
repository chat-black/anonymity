# Introduce
This directory contains experimental data and experimental scripts related to Exp4.

- **tpch_22/** is the execution information of tpch_22.sql under different filtering algorithms and different filtering thresholds.
    - **tpch_22/GFP** is the information at different thresholds using the GFP filtering algorithm. For example, **tpch_22/GFP/0.1** is the result of filtering out all plans with structural differences greater than 0.1.
    - **tpch_22/Cost** is the result when using **GFP & Cost** filtering algorithm.
    - **tpch_22/filter_percentage.png** is percentage of plans filtered out by different algorithms.
    - **tpch_22/plan_interestingness.png** is the results of different filtering algorithms. This one is a little different from the picture in the paper. This is because the data in the paper are experimental results when `lambda` is set to 0 and we have put the data in this case into the **tpch_22_lambda_0** directory.
    - **tpch_22/time.png** is the execution time of different algorithms.
- **tpch_22_lambda_0** is similar to the content in the **tpch_22**, except that `lambda` is set to 0 at this time, while in the experiment of **tpch_22**, `lambda` is the standard value 0.5
- **sql** contains the SQL statement used in this experiment.
- **execute.py** is the experiment script used to perform this experiment. It can automatically execute each statement under **sql** and move the output information to the target location.
- **draw.py** is the plotting script used in this experiment.

# Conduct Experiment
This experiment involves two functions `ARENAGFPExp` and `ARENAGFPFilter` in file *ARENA_system/back_end/gpdb_src/src/backend/gporca/libgpopt/src/engine/CEngine.cpp*. The former is used for this experiment, and the latter implements the **GFP** and **GFP & Cost** strategies. During the experiment, ARENA_system reads the filter parameters of the GFP strategy from */tmp/gtArg*. Therefore, results under different filtering thresholds can be obtained by modifying the content of this file. In addition, the macro definition `#define ARENA_COSTFT` in the file *ARENA_system/back_end/gpdb_src/src/backend/gporca/libgpopt/include/gpopt/search/CTreeMap.h* can control which filtering algorithm is used during the experiment. If this macro is enabled, the **GFP & Cost** filtering algorithm will be used, otherwise the **GFP** filtering method will be used.