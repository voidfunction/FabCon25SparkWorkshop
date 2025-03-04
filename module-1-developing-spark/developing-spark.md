# 🚀 Exercise 1 - Developing Spark Applications  

Welcome to this hands-on lab! In this exercise, you'll explore key concepts and techniques to develop Spark applications efficiently. Let's dive in!  

**What You'll Learn  

By the end of this lab, you'll gain insights into:  

1. Understanding the Medallion Architecture  
  - Learn how the Bronze, Silver, and Gold layers help organize and optimize your data pipelines.  

2. Notebook Development: Choosing the Right Environment  
  - Compare different development environments:  
    - **Fabric UI** vs **VS Code Desktop** vs **VS Code Web**  
    - Understand the differences between **Markdown** and **Code Cells**  

3.Spark Basics: Reading, Transforming, and Writing Data  
  - Load data into a **DataFrame (DF)**  
  - Apply **transformations** and write results efficiently  
  - Stream data **from a directory** in real-time  
  - Explore **PySpark, Scala, and Spark SQL** for processing data  

4. Running & Managing Notebooks  
  - Execute your notebooks in different modes:  
    - **Standard Session** vs **High Concurrency (HC) Session**  
    - Learn how to **extend session expiry** for long-running tasks  

5. Configuring & Publishing Your Spark Environment  
  - Manage **libraries** and dependencies in the UI  
  - Choose between **Starter Pools vs Custom Pools** and understand the difference  
  - Discover how **Autoscaling** and **Dynamic Allocation** work  

6. Using `notebookutils` for Secure Access  
  - Access **Azure Key Vault (AKV)** securely within your notebooks  

---

**Get Ready to Code!**
Now that you have an overview, let's get started with hands-on exercises! 🚀


## 1.1 Understanding the Medallion Architecture  

In this lab, we'll implement the **Medallion Architecture**, a structured approach to organizing data in layers for better performance and reliability:  

- **Bronze Layer**: Raw data is stored in **Azure Data Lake Storage (ADLS)** in **JSON** and **Parquet** formats.  
- **Silver Layer**: Data is cleaned, standardized, and stored in **OneLake** using a **Flattened Delta** format for better querying.  
- **Gold Layer**: Optimized for analytics, and stored in **OneLake**, the Gold layer structures data into **Fact** and **Dimension** tables using **Delta format**.  

This layered approach ensures data is efficiently processed, transformed, and made ready for analysis. 🚀  

---

## 1.2 Notebook Development: Choosing the Right Interface

When developing your Spark applications interactvely, Microsoft Fabric Notebooks offer two flexible options:  
- A **web-based interactive interface** (Fabric UI)  
- **VS Code integration**

Let's explore both! 🚀  

### 1.2.1 Developing in Fabric UI  

The **Fabric UI Notebook** is the easiest way to get started—no setup required! If you have **contributor access** to a Fabric workspace, you can create and run notebooks directly in your browser.  

#### How to Create a Notebook in Fabric UI  
1. Click the **Fabric logo** in the bottom-left corner of the screen.

![Fabric UI](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1a.jpg)  

2. You'll see options for **Fabric** and **Power BI**—select **Fabric**.  

![Select Fabric in the Option](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1b.jpg) 

3. Choose your **workspace**.  Click **New Item** → **Notebook** to create a new notebook. 

![Creating a new Notebook](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1c.jpg) 

4. Click next to the **Notebook icon** to rename your notebook.  

![Renaming a Notebook](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1d.jpg) 

That's it! You're ready to start coding in Spark in Fabric Notebook ! ✨  

### 1.2.2 Integrating with Visual Studio Code

### 1.2.3 Understanding Markdown vs. Code Cells
In Fabric Notebook, you can use Markdown and Code cells to enhance your workflow.

1. To insert a new Mardown or Code cell, hover above or below an existing one. You'll see options to add either a Markdown or Code cell—simply select the type you need!

![Renaming a Notebook](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1e.jpg) 

Lets try this with a sample Spark code and add the context in the Markdown. 

2. Adding a Markdown Cell  
Markdown cells help you document your work, making it easier for collaborators and readers to understand your code.  
With Fabric Notebook’s rich Markdown editor, you can:  
- **Add headings and paragraphs** for better structure  
- **Embed images** to enhance explanations  
- **Format text** using bold, italics, and lists  

Now, add a **Markdown cell** and include the following description:  

> ## Simple Test  
> The following Spark code creates and displays a DataFrame with 3 records.

💡 *Now, let's add a sample Spark code in a Code cell!*  

3. Adding a Code Cell with Sample Spark Code
Now, add a **Code cell** and enter the following **PySpark** code:  

~~~python
# Import required libraries
from pyspark.sql import Row

# Create a sample DataFrame with 3 records
data = [Row(id=1, name="Alice", age=25),
        Row(id=2, name="Bob", age=30),
        Row(id=3, name="Charlie", age=35)]

df = spark.createDataFrame(data)

# Display the DataFrame
df.show()
~~~

4. To execute a cell, simply click **Connect** and choose either:
   - **Run All** to execute all cells in the notebook, or
   - **Run** (located on the left of the cell) to run a specific cell only.

![Executing Cells](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1f.jpg)

5. After running a cell, you'll see the **status** and **results** displayed below the respective cells.

![Results Display](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1g.jpg)

💡 *Click on Spark Jobs, Resources, and Log to dive deeper into the run details. You’ll explore these in the upcoming labs.*

**Awesome!** You've successfully added a Markdown cell for documentation and a Code cell to run Spark code. Now, go ahead and try modifying the dataset or adding some transformations to explore more!

---