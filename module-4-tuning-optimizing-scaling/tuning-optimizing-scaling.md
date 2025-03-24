# Module 4: Performance Tuning, Optimization & Scaling

> [!NOTE]
> Timebox: 60 minutes (20 minutes of content | 40 minutes of lab work)
> 
> [Back to Agenda](./../README.md#agenda) | [Back to Start Steps](../module-0-setup/start.md) | [Up next Exercise 2](./../exercise-2/exercise-2.md)
> #### List of exercises:
> * [Task 4.2 Leveraging the Native Execution Engine](#task-42-leveraging-the-native-execution-engine-5-min)
> * [Task 4.3 Enabling Workload Appropriate Features](#task-43-enabling-workload-appropriate-features-8-min)
> * [Task 4.5 Create Custom Spark Pool](#task-45-create-custom-spark-pool)
> * [Task 4.6](#task-14-management-of-spark-sessions)
> * [Task 4.7](#task-14-management-of-spark-sessions)

# Context
In this exercise, we will explore how to tune, optimize, and scale Fabric Spark workloads, using what we built in the prior exercises as our baseline.

# 4.1 - Introduction & Key Performance Factors | 🕑 2:00 - 2:05 PM
## 📌 Presentation (5 min.)
- Why performance tuning?
- Key areas of optimization:
- Top 10 reasons for poor performance

# 4.2 - Native Execution Engine | 🕑 2:05 - 2:10 PM
## 📌 Presentation (5 min.)

## 🛠️ Task 4.2 - Leveraging the Native Execution Engine (5 min.)


# 4.3 - Optimizing Zone and Use Case Appropriate Table Features | 🕑 2:15 - 2:30 PM
## 📌 Presentation (7 min)
- Different medallion zones and workloads may require different features enabled to optimize performance
    - _slide showing objectives of different zones w/ features mapped_
    - _slide showing objectives of different workload types (i.e. batch vs. streaming vs. reporting) w/ features mapped_

> [!NOTE]
> Fabric Spark Runtimes over time will evolve to automatically optimize when various features are enabled or disabled. These platform and use case dependent defaults optimize for the norm but it can be helpful to understand what individual features to for when encountering edge use cases.

## 🛠️ Task 4.3 - Enabling Workload Appropriate Features (8 min)
- task related to V-Order, OptimizeWrite, AutoCompaction -> THIS NEEDS THOUGHT
- How to audit and verify features are enabled
- Check perf result w/ features enabled

# 4.4 - Table Clustering | 🕑 2:30 - 2:35 PM
## 📌 Presentation (5 min)
- 1 or 2 slides highlighting
    - use cases for data clustering (covering partitioning and liquid clustering)
    - tips for selecting clustering columns
    - why it doesn't make sense to partition below 1TB

# 4.5 - Compute Sizing | 🕑 2:35 - 2:45 PM
## 📌 Presentation (5 min)
- _start custom pool in prior task_
- switch to using larger compute and process a large job (showcase power of Spark)


# 4.6 - Query Optimization | 🕑 2:45 - 2:55 PM
## 📌 Presentation (5 min)
## 🛠️ Task 4.6 - Optimizing  (5 min)
1. repartition scenario
1. cache scenario

# 4.7 - Table Maintenance | 🕑 2:35 - 2:45 PM
## 📌 Presentation (5 min)
## 🛠️ Task 4.7 - Running Table Maintenance Operations (5 min)
- Run function to evaluate count of compacted vs. uncompacted files
- Enable auto-compaction
- Perform write into table
- Run function to validate files were compacted
> [!NOTE]
> When Auto-Compaction is triggered, a background `OPTIMIZE` job is triggered that runs syncronously after the write operation. Auto-Compaction jobs can be be identified via the below:
```python
history_df = spark.sql("DESCRIBE HISTORY dbo.table_with_ac_enabled")
  filtered_history = history_df \
      .filter(history_df.operation == "OPTIMIZE") \
      .filter(history_df.operationParameters.auto == "true")
  display(filtered_history)
```


















