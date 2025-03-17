> [!NOTE]
> Timebox: 60 minutes (20 minutes of content | 40 minutes of lab work)
> 
> [Back to Agenda](./../README.md#agenda) | [Back to Start Steps](../module-0-setup/start.md) | [Up next Exercise 2](./../exercise-2/exercise-2.md)
> #### List of exercises:
> * [Task 2.2 Add Notebook into pipeline](#orchestrating-as-notebook-activity)
> * [Task 2.3 Enable Notebook schedule on Notebook settings page](#23-enable-notebook-schedule-on-notebook-settings-page)
> * [Task 2.4.1 Reference notebook via %run](#241-reference-notebook-via-run)
> * [Task 2.4.2 Reference a notebook via ```notebookutils.notebook.run```](#242-reference-a-notebook-via-notebookutilsnotebookrun)
> * [Task 2.4.3 2.4.3 Reference multi notebooks via ```notebookutils.notebook.runMultiple```](#243-reference-multi-notebooks-via-notebookutilsnotebookrunmultiple)
> * [Task 2.5 Notebook resoures](#25-notebook-resources)

# Module 2: Orchestrating Spark (Long, Miles)
- Notebooks vs. SJDs
- Orchestration patterns -> Pipelines vs. Run vs. RunMultiple vs. ThreadPools
- packaging code (libraries, modules in resources folder)


# Context
In this exercise, we will explore how to orchestrate Spark workloads using Azure Data Factory Notebook Activity and Fabric Scheduler. And meanwhile, we provide multiple ways (NotebookUtils.run, RunMultiple, ThreadPools) to reference a notebook in your pipeline.
we will also explore how to use resource files to package code.


# 2.1 - Introduction & Key features
## 📌 Presentation (5 min.)

# 2.2 Add notebook into pipeline
The Notebook activity in pipeline allows you to run Notebook created in Microsoft Fabric. You can create a Notebook activity directly through the Fabric user interface. This article provides a step-by-step walkthrough that describes how to create a Notebook activity using the Data Factory user interface.

## [Orchestrating as Notebook Activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity)
![](./Add%20to%20pipeline.jpg)

Select the Settings tab, select an existing notebook from the Notebook dropdown, and optionally specify any parameters to pass to the notebook.
![](./Pass%20parameters%20from%20Notebook%20activity%20.jpg)

# 2.3 Enable Notebook schedule on Notebook settings page
![](./Schedule%20Notebook.jpg)

# 2.4 Reference Notebook

We can reference another notebook within current notebook's context.

## 2.4.1 reference notebook via [```%run```](https://learn.microsoft.com/en-us/fabric/data-engineering/author-execute-notebook#reference-run-a-notebook)
The ```%run``` command also allows you to run Python or SQL files that are stored in the notebook’s built-in resources, so you can execute your source code files in notebook conveniently.

```%run [-b/--builtin -c/--current] [script_file.py/.sql] [variables ...]```

![magic run](./Reference%20notebook%20via%20magic%20run.jpg)

For options:

```-b/--builtin```: This option indicates that the command finds and runs the specified script file from the notebook’s built-in resources.

```-c/--current```: This option ensures that the command always uses the current notebook’s built-in resources, even if the current notebook is referenced by other notebooks.

## 2.4.2 Reference a notebook via [```notebookutils.notebook.run```](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-utilities#reference-a-notebook)
This method references a notebook and returns its exit value. You can run nesting function calls in a notebook interactively or in a pipeline. The notebook being referenced runs on the Spark pool of the notebook that calls this function.

```notebookutils.notebook.run("notebook name", <timeoutSeconds>, <parameterMap>, <workspaceId>)```
![nbutils.run](./Reference%20notebook%20via%20nbutils.jpg)

## 2.4.3 Reference multi notebooks via [```notebookutils.notebook.runMultiple```](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-utilities#reference-run-multiple-notebooks-in-parallel)
The method ```notebookutils.notebook.runMultiple()``` allows you to run multiple notebooks in parallel or with a predefined topological structure. The API is using a multi-thread implementation mechanism within a spark session, which means the reference notebook runs share the compute resources.
```notebookutils.notebook.runMultiple(["NotebookSimple", "NotebookSimple2"])```

![nbutils.multirun](./Reference%20multi%20notebooks%20via%20nbutils.jpg)

# 2.5 Notebook resources
The notebook resource explorer provides a Unix-like file system to help you manage your folders and files. It offers a writeable file system space where you can store small-sized files, such as code modules, semantic models, and images. You can easily access them with code in the notebook as if you were working with your local file system.

![nb.resources.operations](./notebook-resources-operations.gif)