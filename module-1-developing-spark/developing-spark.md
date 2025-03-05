# 🚀 Exercise 1 - Developing Spark Applications  

Welcome to this hands-on lab! In this exercise, you'll explore key concepts and techniques for building efficient Spark applications. Get ready to dive in!  

## 🔥 How to Get Started  
You have two ways to approach this lab:  
**Option 1:** Create a new Notebook and follow the step-by-step instructions.  
**Option 2:** Download the pre-built Notebook, run the code, and experiment with it.

## 🎯 What You'll Learn 

By the end of this lab, you'll gain insights into:  

#### <span style="color:blue;">1.1 Understanding the Medallion Architecture</span>

#### <span style="color:blue;">1.2 Notebook Development: Choosing the Right Environment</span>
Compare different development environments:
- **1.2.1** **Fabric UI** vs **VS Code Desktop** vs **VS Code Web**
- **1.2.2** Understand the differences between **Markdown** and **Code Cells**

#### <span style="color:blue;">1.3 Spark Basics: Reading, Transforming, and Writing Data</span>
- **1.3.1** Load data into a **DataFrame (DF)**
- **1.3.2** Apply **transformations** and write results efficiently

#### <span style="color:blue;">1.4 Running & Managing Notebooks</span>
- **1.4.1** Execute your notebooks in different modes: **Standard Session** vs **High Concurrency (HC) Session**

#### <span style="color:blue;">1.5 Configuring & Publishing Your Spark Environment</span>
- **1.5.1** Manage **libraries** and dependencies in the UI
- **1.5.2** Choose between **Starter Pools** vs **Custom Pools** and understand the difference
- **1.5.3** Discover how **Autoscaling** and **Dynamic Allocation** work

#### <span style="color:blue;">1.6 Using `notebookutils` for Secure Access</span>
- **1.6.1** Access **Azure Key Vault (AKV)** securely within your notebooks

---

**Get Ready to Code!**
Now that you have an overview, let's get started with hands-on exercises! 🚀


## 1.1 Understanding the Medallion Architecture  

In this lab, we'll implement the **Medallion Architecture** in this lab, a structured approach to organizing data in layers for better performance and reliability:  

- **Bronze Layer**: data lands into this zone directly from the source systems in its original format.  This zone is generally considered append only and immutable. For this lab, raw data is stored in **Azure Data Lake Storage (ADLS)** in **JSON** and **Parquet** formats. 

- **Silver Layer**: Data is cleaned, standardized, and stored in **OneLake** using a **Flattened Delta** format for better querying. 

- **Gold Layer**: Optimized for analytics, and stored in **OneLake**. It can have:
    - Denormalized Tables – Optimized for fast querying in reporting tools like Power BI. These tables combine fact and dimension data to reduce the need for joins.
    - Fact and Dimension Tables – A more normalized approach, useful for maintaining data integrity and flexibility while still being optimized for analytics.

  The choice depends on your use case:
  - Denormalized tables are best for performance and ease of use in reporting.
  - Fact and dimension tables provide flexibility and are useful for more complex analytical models. 

This layered approach ensures data is efficiently processed, transformed, and made ready for analysis. 🚀  

---

## 1.2 Notebook Development: Choosing the Right Interface [10 minutes]

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

[To be Updated]

### 1.2.3 Understanding Markdown vs. Code Cells
In Fabric Notebook, you can use Markdown and Code cells to enhance your development and collaboration. 

1. To insert a new Markdown or Code cell, hover above or below an existing one. You'll see options to add either a Markdown or Code cell—simply select the type you need!

![Insert a Markdown](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1e.jpg) 

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

6. **Using PySpark, Scala, or Spark SQL**: 
You can use PySpark, Scala, or Spark SQL for processing data. Choose the language you are most comfortable with. By default, the Notebook will show the PySpark kernel. 

You can change the kernel at the notebook level or cell level with:

- %%spark for Scala.
- %%pyspark for PySpark.
- %%sql for Spark SQL.

![Language Support](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1g.jpg)


7. **Extending Session Timeout**:
By default, session timeout is 20 minutes. You can extend it while working on development. To extend session expiry, click on the Session Ready status. It will display session information, and you can click reset and add time.

**Awesome!** You've successfully added a Markdown cell for documentation and a Code cell to run Spark code. Now, go ahead and try modifying the dataset or adding some transformations to explore more!

---

## 1.3 Spark Basics: Reading, Transforming, and Writing Data with Bronze, Silver, and Gold Layers [20 minutes]

  ### 1.3.1 Bronze Layer: Load Raw Data into a **DataFrame (DF)**

  In Spark, you can load data into a DataFrame using the `spark.read()` method. This allows you to read from a variety of source formats such as CSV, JSON, Parquet, Avro, ORC, Delta, and many others. 

  In this lab, we'll focus on reading **Parquet** files and **streaming JSON** files.


  #### **Load JSON Data into a DataFrame**

  To begin, let's load a JSON file from Azure Data Lake Storage (ADLS) into a Spark DataFrame. 

  In a new code cell, use the following code to:
  - read the JSON files
  - print the schema
  - display the data

  ~~~python
  # Load JSON data from ADLS
  input_df = spark.read.option("multiline", "true").json(adls_path)

  # Show the schema of the DataFrame
  input_df.printSchema()

  # Display the data (first few rows)
  input_df.show(truncate=False)
  ~~~

  **printSchema()**: Prints the schema of the DataFrame, giving you an overview of the data structure.

  **show(truncate=False)**: Displays the content of the DataFrame without truncating long values.

  #### **Read Parquet Data**

  Let's load Parquet data from ADLS and display the first 10 records:

  ~~~python
  # Read Parquet file into a DataFrame
  df = spark.read.parquet("path/to/parquet_file.parquet")

  # Show first 10 records
  df.show(10, truncate=False)
  ~~~

  ### 1.3.2 Silver Layer: Cleaning and De-duplicating Data

  Now that you've loaded both JSON and Parquet data to Dataframe, let’s clean and flatten data from both the Order and Product tables and prepare them in the Silver Layer.

  **Remove duplicates** from the orders dataframe and **cleaning** products dataframe

  ~~~python
  from pyspark.sql import functions as F

  # Remove duplicates based on all columns in the 'orders_df'
  orders_df_cleaned = order_df.dropDuplicates()

  # Show the cleaned data to verify
  orders_df_cleaned.show(truncate=False)

  # Filter out rows where ProductID or ProductName is null
  filtered_products_df = product_df.filter(
      (F.col("ProductID").isNotNull()) & (F.col("ProductName").isNotNull())
  )

  # Show the filtered data to verify
  filtered_products_df.show(truncate=False)
  ~~~

### 1.3.3 Gold Layer: Creating a Denormalized Table

  Perform an INNER JOIN on ProductID to get enriched Order Details and save in Gold Layer. 

  ~~~python
  gold_df = orders_df.join(products_df, "ProductID", "inner")
  ~~~

### 1.4 Running & Managing Notebooks  [5 minutes]  

#### 1.4.1 Execute Notebooks in Different Modes  

Fabric offers two types of sessions for running Spark Notebooks, each optimized for different use cases:  

- **Standard Session**: Runs a single Spark Notebook per session.  
- **High Concurrency (HC) Session**: Allows multiple Notebooks to share the same session when run by the same user, with the same environment and attached Lakehouse.  

This significantly **reduces cumulative session startup time**, improving developer productivity.  

#### **Running a Standard Session**  
1. Click on the **Standard Session** icon.  
2. Select **New Standard Session** and run your Notebook.  

#### **Testing High Concurrency (HC) Mode**  
1. Create **two Notebooks** (Notebook 1 and Notebook 2) for testing.  
2. In **Notebook 1**, start a **High Concurrency Session**:  
   - Click on the **Standard Session** icon.  
   - Choose **New High Concurrency Session** and run Notebook 1.  
3. In **Notebook 2**, check the available sessions:  
   - Click on the **Standard Session** icon.  
   - You will see the **existing High Concurrency session** available for attachment.  
   - Attach Notebook 2 to the same session and run it.  

This approach **optimizes resource usage and accelerates execution times**, enhancing your interactive data workflows! 🚀  

### 1.5 Configuring & Publishing Your Spark Environment [15 minutes] 
  - Manage **libraries** and dependencies in the UI  
  - Choose between **Starter Pools vs Custom Pools** and understand the difference  
  - Discover how **Autoscaling** and **Dynamic Allocation** work  

  [To be updated]

### 1.6 Using `notebookutils` for Secure Access  [5 minutes]
  - Access **Azure Key Vault (AKV)** securely within your notebooks
  [To be updated]  

### Bonus

#### Read Streaming JSON Data
In this Bonus section, we will walk through how to read streaming JSON data from Azure Data Lake Storage (ADLS) using Apache Spark's structured streaming capabilities. This method allows Spark to continuously read new JSON files as they arrive, while ignoring the files that have already been processed. It's especially useful for processing real-time data streams.

1. **Define the Path to Your Data**: To begin, we specify the path to the JSON files stored in ADLS. The path includes a wildcard (*.json) to ensure that all JSON files in the specified directory are processed.

2. **Set Up the Spark ReadStream**: With the file path defined, we use spark.readStream to start a streaming read operation. The option("multiline", "true") option ensures that Spark can correctly handle multi-line JSON files. Then, the .json() method reads the incoming JSON data from the specified path.

    ~~~python
    input_df = spark.readStream.option("multiline", "true").json(adls_path_json)
    ~~~

3. **Defining the Schema**: When working with streaming data, it’s important to define a schema to know the structure of the incoming data. In this case, we define two schemas:

- Outer schema: Defines the top-level structure, which includes the json_data field (containing a JSON string).
- Inner schema: Defines the actual data within the json_data field (which in this example contains fields like OrderID and Product).

    ~~~python
    outer_json_schema = StructType([
        StructField("json_data", StringType(), True)
    ])

    inner_json_schema = StructType([
        StructField("OrderID", StringType(), True),
        StructField("Product", StringType(), True)
    ])
    ~~~
4. **Processing the Stream**: 

      - We use spark.readStream with the schema outer_json_schema to ensure the incoming JSON is read according to the defined structure. The stream is read from the ADLS directory where the JSON files are stored.

      ~~~python
      input_df = spark.readStream.schema(outer_json_schema).json(adls_path)
      ~~~
      - The next step is to parse the JSON data inside the json_data field. Using the from_json function, we transform the string of JSON data into a structured format using the inner_json_schema.

      ~~~python
      parsed_df = input_df.withColumn("parsed_json", from_json(col("json_data"), inner_json_schema)) \
                          .select("parsed_json.*")
      ~~~
This extracts the OrderID and Product fields from the json_data column.

5. Define the process_batch Function to show the data in the batch for debugging and write the processed data to a Delta table.

      ~~~python
      def process_batch(df, epoch_id):
          df.show(truncate=False)
          df.write.format("delta").mode("append").saveAsTable("orders1")
      ~~~
6. **Set Up the Streaming Query**: Use .foreachBatch to apply the process_batch function to each micro-batch. Configure checkpointing for fault tolerance.
      ~~~python
      query = parsed_df.writeStream \
          .outputMode("append") \
          .foreachBatch(process_batch) \
          .option("checkpointLocation", "abfss://fabricconlab@vengcatadls001.dfs.core.windows.net/jsonstreaming/checkpoints/") \
          .start()
      ~~~

**Tip**: Don't forget to stop the stream once you're done testing, as it will continue running indefinitely.
