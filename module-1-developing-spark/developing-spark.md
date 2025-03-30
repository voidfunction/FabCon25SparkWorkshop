# 🚀 Exercise 1 - Developing Spark Applications  

Welcome to this hands-on lab! In this exercise, you'll explore key concepts and techniques for building efficient Spark applications. Let's dive in!  

## 🔥 How to Get Started  
You have two ways to approach this lab:  
**Option 1:** Create a new Notebook and follow the step-by-step instructions.  
**Option 2:** Download the pre-built Notebook [module-1-developing-spark/Lab 1 - Developing Spark Applications.ipynb](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/module-1-developing-spark/Lab%201%20-%20Developing%20Spark%20Applications.ipynb), run the code, and experiment with it.

Create two Lakehouses silver and bronze by clicking on "Add data items":
![Fabric UI](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.1.1a1.jpg?raw=true)  

#### 📌 Presentation (5 min.)

## 🎯 What You'll Learn 

By the end of this lab, you'll gain insights into:  
- The basics of how Spark works
- The _Medallion_ architecture
- Development environments: Fabric UI, VS Code Desktop, and VS Code Web
- The differences between Markdown and Code Cells
- How to read, transform, and write data using Spark DataFrames
- Running notebooks in Standard and High Concurrency modes
- Spark Environments: manage libraries, choose pools, and explore autoscaling

---

**Get Ready to Code!**
Now that you have an overview, let's get started with hands-on exercises! 🚀


##### **Preparation**  
1. Create two Lakehouses (with schema): `silver` and `gold` in your workspace.  
   To create Lakehouses with schema, click on **+Lakehouses** in the left sidebar, then select **Add New Lakehouse**. 

![Create Lakehouse](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/preparation1.jpg?raw=true) 

![Create Lakehouse](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/preparation2.jpg?raw=true) 

![Create Lakehouse](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/preparation3.jpg?raw=true) 
   
2. Make **silver** the default Lakehouse for this notebook.  
   This can be done by selecting **silver** as the default in the Lakehouse section in the left as shown in the screenshot below:

![Setting Default Lakehouse](![Create Lakehouse](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/preparation4.jpg?raw=true) ) 

**Important:** Ensure that both `silver` and `gold` Lakehouses are created before proceeding with the exercises, as they will be used in the upcoming steps.


## 1.1 Understanding the Medallion Architecture  

In this lab, we'll implement the **Medallion Architecture**, a structured approach to organizing data in layers for better performance and reliability:  

- **Bronze Layer** : This layer serves as the initial landing zone for data from the source systems, preserving its original format. This zone is typically **append-only** and **immutable**.  

- **Silver Layer**: Data is cleaned, standardized, and stored in **OneLake** in **Flattened Delta** format for better querying. 

- **Gold Layer**: Optimized for analytics, and stored in **OneLake**. It can have denormalized tables or fact and dimension tables :
    - Denormalized Tables – Optimized for fast querying in reporting tools like Power BI. These tables combine fact and dimension data to reduce the need for joins.
    - Fact and Dimension Tables – A more normalized approach, useful for maintaining data integrity and flexibility while still being optimized for analytics.

  The choice depends on your use case:
  - Denormalized tables are best for performance and ease of use in reporting.
  - Fact and dimension tables provide flexibility and are useful for more complex analytical models. 

**However, there is no single correct interpretation of the Medallion layers. Organizations decide the architecture based on their specific data needs:
- Some organizations introduce more than three layers to support additional transformations or governance requirements.
- Some use Bronze for Delta storage, treating Silver as a logical layer with views rather than a separate physical storage layer.
Ultimately, the goal is to create value from data while ensuring logical organization of transformations to support replayability, comprehensive auditing, and scalable data consumption at the required level of cleanliness.**

![MEDALLION LAYERS](https://learn.microsoft.com/en-us/fabric/onelake/media/onelake-medallion-lakehouse-architecture/onelake-medallion-lakehouse-architecture-example.png)

This layered approach ensures data is efficiently processed, transformed, and made ready for analysis. 🚀  

---

## 1.2 Notebook Development: Choosing the Right Interface (20 minutes)

When developing your Spark applications interactvely, Microsoft Fabric Notebooks offer two flexible options:  
- A **web-based interactive interface** (Fabric UI)  
- **VS Code integration**

Let's explore both! 🚀  

### 1.2.1 Developing in Fabric UI  

The **Fabric UI Notebook** is the easiest way to get started—no setup required! If you have **contributor access** to a Fabric workspace, you can create and run notebooks directly in your browser.  

#### How to Create a Notebook in Fabric UI  
1. Click the **Fabric logo** in the bottom-left corner of the screen.

![Fabric UI](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1a.jpg?raw=true)  

2. You'll see options for **Fabric** and **Power BI**—select **Fabric**.  

![Select Fabric in the Option](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1b.jpg?raw=true) 

3. Choose your **workspace**.  Click **New Item** → **Notebook** to create a new notebook. 

![Creating a new Notebook](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1c.jpg?raw=true) 

Alternatively, if you're importing a pre-built Notebook, click on import.

![Importing Notebook](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.1.1a.jpg?raw=true)


4. Click next to the **Notebook icon** to rename your notebook.  

![Renaming a Notebook](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1d.jpg?raw=true) 

That's it! You're ready to start coding in Spark in Fabric Notebook ! ✨  

### 1.2.2 Integrating with Visual Studio Code (🛠 Presentation, No Hands-On)

🎥 **See the Visual Studio integration in action!**  
👉 [Click here to watch the video](https://www.youtube.com/watch?v=7TGsTd1SdoU)  

Here are the detailed steps:

You can integrate with Visual studio desktop or web. For this lab, we will set up Visual Studio web. 

#### 1. Install the Fabric Data Engineering VS Code extension for the Web

Navigate to https://vscode.dev in your browser.

![vscode.dev](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.1.1b.jpg?raw=true) 

Select the Extensions icon in the left navigation bar.

![Fabric Engineering Extension](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.1.1c.jpg?raw=true) 

Search for Fabric Data Engineering and select the Fabric Data Engineering VS Code - Remote pre-release

![Fabric Engineering Extension](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.1.1d.jpg?raw=true) 

click Install.

![Installing Fabric Engineering Extension](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.1.1e.jpg?raw=true) 

#### 2. Open the Notebook with VS Code Web

Open your current notebook in the VS Code for the Web experience by clicking the Open in VS Code(Web) button in the notebook authoring page in the Fabric portal.

![Open Notebook with VS Code Web](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.1.2b.jpg?raw=true) 

select Fabric Runtime as the kernel and then select PySpark.

If configured correctly, you will see Fabric Runtime in the selected kernel. 

![Selected Kernel](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.1.2e.jpg?raw=true) 

If you're have a new notebook, you can test execution by adding a simple command such as:

~~~python
print(5)
~~~

Then execute the cell. You can update a cell and ctrl + s to save the work. 

![Selected Kernel](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.1.2f.jpg?raw=true) 

![Updating Notebook](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.1.2g.jpg?raw=true) 

***Note**: Let's continue doing our labs in Notebook in Fabric UI.*

### 1.2.3 Understanding Markdown vs. Code Cells
In Fabric Notebook, you can use Markdown and Code cells to enhance your development and collaboration. 

1. To insert a new Markdown or Code cell, hover above or below an existing one. You'll see options to add either a Markdown or Code cell—simply select the type you need!

![Insert a Markdown](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1e.jpg?raw=true) 

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
  display(df)
  ~~~

4. To execute a cell, simply click **Connect** and choose either:
   - **Run All** to execute all cells in the notebook, or
   - **Run** (located on the left of the cell) to run a specific cell only.

![Executing Cells](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1f.jpg?raw=true)

5. After running a cell, you'll see the **status** and **results** displayed below the respective cells.

![Results Display](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1g.jpg?raw=true)

💡 *Click on Spark Jobs, Resources, and Log to dive deeper into the run details. You’ll explore these in the upcoming labs.*

6. **Using PySpark, Scala, or Spark SQL**: 
You can use PySpark, Scala, or Spark SQL for processing data. Choose the language you are most comfortable with. By default, the Notebook will show the PySpark kernel. 

You can change the kernel at the notebook level or cell level with:

- %%spark for Scala.
- %%pyspark for PySpark.
- %%sql for Spark SQL.

![Language Support](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.2.1g.jpg?raw=true)

## 🔥 Challenge Yourself with PySpark!  

Below is a PySpark code snippet that defines a list named `data`, which contains three rows:  

- 🟢 **First row:** `id=1`, `name="Alice"`, `age=25`  
- 🔵 **Second row:** `id=2`, `name="Bob"`, `age=30`  
- 🟠 **Third row:** `id=3`, `name="Charlie"`, `age=35` 

  ~~~python
  # Import required libraries
  %%pyspark
    # Import required libraries
    from pyspark.sql import Row

    # Create a sample DataFrame with 3 records
    data = [Row(id=1, name="Alice", age=25),
            Row(id=2, name="Bob", age=30),
            Row(id=3, name="Charlie", age=35)]
  ~~~

  ## 💡 Your Task:
Write a PySpark code snippet to:  
1️⃣ Create a DataFrame from the data.  
2️⃣ Display all the records in the DataFrame.  
3️⃣ Print the schema of the DataFrame.  

🚀 **Think you got this? Give it a try!**  

🔍 **Hint:** You need to use `spark.createDataFrame()` to convert `data` into a DataFrame.  

<details>
  <summary><strong>🔑 Answer:</strong> Click to reveal</summary>

~~~python
# Create a DataFrame from the data
df = spark.createDataFrame(data)

# Display the DataFrame
display(df)

# Print the Schema of the Dataframe
df.printSchema()
~~~
  
</details>

## 🚀 Scala Spark Challenge!  

Here is the equivalent code in **Scala Spark**.  

### 📝 **Your Task:**  
To run the Scala code, use the `%%spark` magic command like below.  

📌 **Challenge:** Modify the code to **pass a static schema** instead of using schema inference.  

🔹 **Step 1:** Run the following code below.  
🔹 **Step 2:** Copy and Modify the code in a new cell to define a **manual schema** instead of inferring it.  

💡 **Hint:** Use `StructType` and `StructField` to define the schema explicitly.  

---

### ✅ **Run this code to Start!** 

~~~scala
%%spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
 
val data = Seq(
  (1, "Alice", 25),
  (2, "Bob", 30),
  (3, "Charlie", 35)
)
 
val df = spark.createDataFrame(data).toDF("id", "name", "age")
 
// Show DataFrame
display(df)
~~~

The toDF() method in Spark is a convenience method that allows you to easily convert a collections (such as a Seq, List, or RDD) into a DataFrame and assign column names.

Now, Copy and Modify the code in a new cell to define a **manual schema** instead of inferring it. 

<details>
  <summary><strong>🔑 Answer:</strong> Click to reveal</summary>

~~~scala
%%spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// Define schema
val schema = StructType(Seq(
  StructField("id", IntegerType, nullable = false),
  StructField("name", StringType, nullable = false),
  StructField("age", IntegerType, nullable = false)
))

// Create sample data
val data = Seq(
  Row(1, "Alice", 25),
  Row(2, "Bob", 30),
  Row(3, "Charlie", 35)
)

// Create DataFrame
val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

display(df)

~~~
</details> 

## 🎯 **Challenge: Spark SQL Edition!**

Want to try your hand at Spark SQL? It’s time to switch gears and use SQL to achieve the same goal! 😎

### **Here’s what you need to do:**

Instead of using PySpark code, you’ll use the `%%sql` magic command to create a **temporary view** and run SQL queries.

🔥 **Step 1**: Run the SQL below to create a temporary view and insert data.

🔥 **Step 2**: Run this SQL query to select all records from the people view.

<details>
  <summary><strong>🔑 Answer:</strong> Click to reveal</summary>

~~~sql

%%sql

CREATE OR REPLACE TEMP VIEW people AS
SELECT 1 AS id, 'Alice' AS name, 25 AS age UNION ALL
SELECT 2 AS id, 'Bob' AS name, 30 AS age UNION ALL
SELECT 3 AS id, 'Charlie' AS name, 35 AS age;

SELECT * FROM people

~~~
</details> 

7. **Extending Session Timeout**:
By default, session timeout is 20 minutes. You can extend it while working on development. To extend session expiry, click on the Session Ready status. It will display session information, and you can click reset and add time.

![Session Timeout](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.3.1b.jpg?raw=true)

💡**Note**: The Session Ready icon will only be visible if a session is currently running 

**Awesome!** You've successfully added a Markdown cell for documentation and a Code cell to run Spark code. Now, go ahead and try modifying the dataset or adding some transformations to explore more!

---

## 1.3 Spark Basics: Reading, Transforming, and Writing Data with Bronze, Silver, and Gold Layers (20 minutes)

  ### 1.3.1 Bronze Layer: Load Raw Data into a **DataFrame (DF)**

  We have raw **FHIR Patient** and **Observations** data are stored in **bronze Lakehouse**. We have given you read access to this Lakehouse. 
  
  For this lab, you can acess the raw data **observationsraw** and **patientraw** in **bronze** Lakehouse. 

  To add **bronze** lakehouse to the Notebook, click +Lakehouses and select the bronze in existing Lakehouses with schema. 

  ![Attaching Lakehouse](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.3.1a1.jpg?raw=true)

  In Spark, you can load data into a DataFrame using the `spark.read()` method. This allows you to read from a variety of source formats such as CSV, JSON, Parquet, Avro, ORC, Delta, and many others.
  
  You can refer to the Spark documentation for the methods to read different formats: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html

  In this lab, we'll focus on reading **Parquet** files and **streaming JSON** files. We will read the observationsraw (JSON files) and patientraw (Parquet files) from bronze Lakehouse. 

    ![Files in Lakehouse](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.3.1a2.jpg?raw=true)


  #### **Load JSON Data into a DataFrame**

  **What is a Dataframe?**

  In Spark, a DataFrame is a distributed collection of data organized into named columns similar to an SQL table. It is similar to a table in a relational database or a spreadsheet in that it has a schema, which defines the types and names of its columns, and each row represents a single record or observation.

    To begin, let's load a JSON file from bronze Lakehouse files into a Spark DataFrame. 

    In a new code cell, use the following code to:
    - read the JSON files
    - print the schema
    - display the data

  Copy the ABFS path of **observationraw**

  ~~~python
  from pyspark.sql.functions import from_json,col

    observations_path="abfss://47938747-73b4-4f78-99dc-4ff2afa78142@onelake.dfs.fabric.microsoft.com/443d992d-01f1-4caa-9345-088e81dd81df/Files/observationsraw"
    # Load JSON data
    observations_raw_df = spark.read.json(observations_path)

    observations_raw_df.printSchema()

    display(observations_raw_df)
  ~~~

  **printSchema()**: Prints the schema of the DataFrame, giving you an overview of the data structure.

  **show(truncate=False)**: Displays the content of the DataFrame without truncating long values.

  #### **Read Parquet Data**

  Let's load Parquet data from **patientraw** and display the first 10 records:

  ![Path of patientraw shortcut](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.3.1a4.jpg?raw=true)

  ~~~python
  patient_path = "abfss://47938747-73b4-4f78-99dc-4ff2afa78142@onelake.dfs.fabric.microsoft.com/443d992d-01f1-4caa-9345-088e81dd81df/Files/patientraw"

    # Read Parquet files into a DataFrame
    patient_raw_df = spark.read.parquet(patient_path)

    patient_raw_df.printSchema()

    # Show the contents of the DataFrame
    display(patient_raw_df)
  ~~~

  ### 1.3.2 Silver Layer: Cleaning, De-duplicating, and Flattening Data

Now that we've loaded both JSON and Parquet data into DataFrames, let's clean the data and flatten nested structures like arrays and structs in the Observations and Patient datasets to prepare them for the Silver Layer.

To enable SQL queries on Spark DataFrames, we will create temporary views using .createOrReplaceTempView().

  ~~~python
  observations_raw_df.createOrReplaceTempView("observations_raw_view")
  patient_raw_df.createOrReplaceTempView("patient_raw_view")
  ~~~

  #### Understanding explode() and Accessing Structs in Spark
In Spark, the explode() function is designed specifically to work with arrays. If you have a column that is a struct, you cannot use explode() directly. Instead, you can access its fields by using dot notation.

For example, column encounter is a struct, you can access its field reference like this:

~~~sql
SELECT encounter.reference FROM observations_raw_view;
~~~

To flatten an array, you can use the explode() function. 

But what if you have an array of structs? To flatten and access an array of structs, you can use the LATERAL VIEW EXPLODE() combination.

For example, to explode the category array and access its coding field, use:

~~~sql
SELECT category_exploded.coding
FROM observations_raw_view
LATERAL VIEW EXPLODE(category) AS category_exploded;
~~~
This method allows you to break down arrays of complex types, like structs, into individual rows—making your data easier to work with.

#### Flattening and Selecting Key Columns in the Silver Layer

In the Silver Layer, we typically flatten the tables from the Bronze Layer while retaining important columns. Flattening helps normalize nested structures like arrays and structs, making the data easier to query and process.

However, for simplicity in this lab, we will write an SQL query to extract only a few key columns from the `observations_raw_view`, rather than keeping all columns from the Bronze Layer.

In `observations_raw_view`, since `category` is an array, we use `LATERAL VIEW OUTER EXPLODE(category)` to extract its elements, keeping null values if the array is empty.

Below is the SQL query to flatten and extract key fields from the `observations_raw_view`:

~~~sql
%%sql

SELECT
    id,
    resourceType,
    status,
    subject.reference AS subject_reference,
    SPLIT(subject.reference, '/')[1] AS patient_id,
    encounter.reference AS encounter_reference,
    
    -- Extract category code and display (first element of array)
    category_struct.coding[0].code AS category_code,
    category_struct.coding[0].display AS category_display,

    -- Extract observation code details
    code.coding[0].code AS observation_code,

    -- Extract effective date
    effectiveDateTime,

    -- Extract value details (Quantity, CodeableConcept, String)
    valueQuantity.value AS value_quantity,
    valueQuantity.unit AS value_unit

FROM observations_raw_view
LATERAL VIEW OUTER EXPLODE(category) category_table AS category_struct
~~~

Next, we'll write a SQL query to extract essential columns from the patient_raw_view and flatten nested struct fields, such as name, using the EXPLODE() function. In the case of name, we will extract the first element from the given array.

~~~sql
%%sql

SELECT 
    p.id,
    p.gender,
    p.birthDate,
    p.deceasedDateTime,

    -- Flatten name details
    name_array.family AS last_name,
    name_array.given[0] AS first_name  -- Extract first element from array

FROM patient_raw_view p
LATERAL VIEW OUTER EXPLODE(p.name) name_table AS name_array
~~~

#### Create and Insert into Flattened Observations Table in the Silver Layer

Next, we will create and insert data into a new observations_silver table in the Silver layer with a flattened schema. 

This table will be stored in the silvercleaned lakehouse, so specifying as silvercleaned.dbo.observations_silver.

~~~sql
%%sql
-- Ensure the Silver table exists (if not, create it)
CREATE TABLE IF NOT EXISTS silvercleansed.dbo.observations_silver
USING DELTA
AS 
SELECT
    id,
    resourceType,
    status,
    subject.reference AS subject_reference,
    SPLIT(subject.reference, '/')[1] AS patient_id,
    encounter.reference AS encounter_reference,
    
    -- Extract category code and display (first element of array)
    category_struct.coding[0].code AS category_code,
    category_struct.coding[0].display AS category_display,

    -- Extract observation code details
    code.coding[0].code AS observation_code,

    -- Extract effective date
    effectiveDateTime,

    -- Extract value details (Quantity, CodeableConcept, String)
    valueQuantity.value AS value_quantity,
    valueQuantity.unit AS value_unit

FROM observations_raw_view
LATERAL VIEW OUTER EXPLODE(category) category_table AS category_struct;
~~~

#### Creating the Patient Silver Table in the Silvercleaned Lakehouse

The following SQL code creates the `patient_silver` table in the `silvercleaned` lakehouse if it doesn't already exist:

~~~sql
%%sql

CREATE TABLE IF NOT EXISTS silvercleaned.dbo.patient_silver (
    id STRING,
    gender STRING,
    birthDate DATE,
    deceasedDateTime TIMESTAMP,
    last_name STRING,
    first_name STRING
)
USING DELTA;
~~~

Writing a SQL code to flatten nested types and insert to the `patient_silver` table:

~~~sql
%%sql
INSERT INTO silvercleansed.dbo.patient_silver

SELECT 
    p.id,
    p.gender,
    p.birthDate,
    p.deceasedDateTime,

    -- Flatten name details
    name_struct.family AS last_name,
    name_struct.given[0] AS first_name  -- Extract first element from array

FROM patient_raw_view p
LATERAL VIEW OUTER EXPLODE(p.name) name_table AS name_struct;
~~~


### 1.3.3 Gold Layer: Creating a Denormalized Table

We will perform an **INNER JOIN** between the `Patient` and `Observations` tables to create and insert to a denormalized table in the Gold Layer. This table will be used for reporting and analytics.

  ~~~sql
  %%sql
-- Creating the Gold Layer with necessary patient and observation details
CREATE TABLE IF NOT EXISTS golddenormalized.dbo.patientobservations_gold
USING DELTA
AS
SELECT
    o.id AS observation_id,
    o.resourceType,
    o.status,
    o.patient_id,
    o.encounter_reference,
    
    -- Patient details from the patient_silver table
    p.gender,
    p.birthDate,
    p.deceasedDateTime,
    p.last_name,
    p.first_name,

    -- Observation specific details
    o.category_code,
    o.category_display,
    o.observation_code,
    o.effectiveDateTime,
    o.value_quantity,
    o.value_unit

FROM silvercleansed.dbo.observations_silver o
JOIN silvercleansed.dbo.patient_silver p
    ON o.patient_id = p.id;
  ~~~

##### Select the Top 10 Patients with the Most Observations

Next, let's query the top 10 patients who have the highest number of observations.

~~~sql
%%sql

SELECT
    patient_id,
    gender,
    COUNT(observation_id) AS number_of_observations
FROM golddenormalized.dbo.patientobservations_gold
GROUP BY
    patient_id, gender
ORDER BY
    number_of_observations DESC
LIMIT 10;
~~~

---

### 1.4 Running & Managing Notebooks  (5 minutes)  

#### 1.4.1 Execute Notebooks in Different Modes  

Fabric offers two types of sessions for running Spark Notebooks, each optimized for different use cases:  

- **Standard Session**: Runs a single Spark Notebook per session.  
- **High Concurrency (HC) Session**: Allows multiple Notebooks to share the same session when run by the same user, with the same environment and attached Lakehouse.  

This significantly **reduces cumulative session startup time**, improving developer productivity.  

#### **Running a Standard Session**  
1. Click on the **Connect** icon.  
2. Select **New Standard Session** and run your Notebook.  

![Session](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.4.1a.jpg?raw=true)

#### **Testing High Concurrency (HC) Mode**  
1. Create **two Notebooks** (Notebook 1 and Notebook 2) for testing.  
2. In **Notebook 1**, start a **High Concurrency Session**:  
   - Click on the **Connect** icon.  
   - Choose **New High Concurrency Session** and run Notebook 1. 

![HC Session](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.4.1b.jpg?raw=true) 

3. In **Notebook 2**, check the available sessions:  
   - Click on the **Connect** icon.  
   - You will see the **existing High Concurrency session** available for attachment.  
   - Attach Notebook 2 to the same session and run it.  

![Attaching to existing HC Session](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.4.1c.jpg?raw=true) 

This approach **optimizes resource usage and accelerates execution times**, enhancing your interactive data workflows! 🚀 

>💡 **Tip:** You can share data across notebooks within the same HC session using a **Global Spark View**.  
>  
> If you create a **Global View** in **Notebook 1** like this:  
>  
> ```python  
> df.createOrReplaceGlobalTempView("global_view")  
> ```  
>  
> You can access it from any other notebook in the same HC session:  
>  
> ```python  
> spark.sql("SELECT * FROM global_temp.global_view")  
> ```

---

### 1.5 Configuring & Publishing Your Spark Environment (🛠 Presentation, No Hands-On)

🎥 **Watch Configuring Custom Pool and Environment in Action**  
👉 [Click here to watch the video](https://www.youtube.com/watch?v=4eaT4zzDxgU)

#### 🚀 Set Up Your Spark Environment with Ease!  

In this lab, you'll explore how to configure and publish your Spark environment efficiently. By the end, you’ll know how to:  

✅ Manage **libraries** and dependencies  
✅ Choose between **Starter Pools vs Custom Pools**  
✅ Leverage **Autoscaling & Dynamic Allocation** for optimal performance  

---

### 🔧 Step 1: Select or Create a Spark Environment  

To get started, open the **Notebook** and click on the **Environment** dropdown, as shown below:  

![Selecting the environment](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.5.1a1.jpg?raw=true)  

If your Spark job requires additional libraries, you can **add public or custom libraries**:  

![Adding custom libraries](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.5.1a.jpg?raw=true)  

Once you've made your changes, remember to **Save & Publish** for them to take effect.  

---

### ⚡ Step 2: Choose the Right Compute Pool  

In **Fabric Spark**, you can choose between:  

- **Starter Pools** (Pre-provisioned, always-on clusters for quick execution)  
- **Custom Pools** (User-defined clusters with flexible scaling options)  

#### ✨ Starter Pools  
Starter pools allow you to run Spark within **seconds** without waiting for nodes to set up. These clusters are **always available**, dynamically scaling up based on job demands.  

#### 🛠️ Custom Spark Pools  
If you need more control, **Custom Pools** let you define the number and size of nodes 

To customize or create a Spark pool, go to **Workspace Spark Settings** (admin access required). For this lab, we’ve already set up a **custom pool**.  

📸 Screenshots for reference:  

![Compute Selection](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.5.2a.jpg?raw=true)  

![Pool Configuration](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.5.2b.jpg?raw=true)  

![Finalize Settings](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.5.2c.jpg?raw=true)  

#### Optimize Application with Autoscaling & Dynamic Allocation  

To ensure efficient resource usage, **Autoscaling** and **Dynamic Allocation** help manage compute resources dynamically.  

#### 🔄 Autoscaling  
- Automatically **scales up or down** based on **YARN pending resources** (Memory & CPU).  
- Adjusts cluster size based on workload demand.  

#### ⚙️ Dynamic Allocation  
- Dynamically **adds or removes executors** based on  tasks backlog and executor idle time.  
- Avoids over-provisioning and reduces idle resources.  

    💡 **How It Works:**  
    - You define **minimum and maximum nodes** for autoscaling.  
    - Spark automatically adjusts nodes based on demand.  
    - The system dynamically assigns executors **only when needed**, ensuring efficiency.  

📸 Selecting Compute Pools:  

![Pool Selection](https://github.com/voidfunction/FabCon25SparkWorkshop/blob/main/screenshots/module-1-developing-spark/1.5.3a.jpg?raw=true)  

---

### 🌟 Bonus - Spark Structured Streaming

#### Read Streaming JSON Data
In this section, we will walk through how to read streaming JSON data from Lakehouse Files using Apache Spark's structured streaming capabilities. This method allows Spark to continuously read new JSON files as they arrive, while ignoring the files that have already been processed. It’s especially useful for processing both historical and real-time data streams.

1. **Define the Path to Your Data**: Start by specifying the ABFS path to the JSON files in bronze Lakehouse.

2. **Set Up the Spark ReadStream**: With the file path defined, use `spark.readStream` to initiate the streaming read operation. The `.json()` method reads the incoming JSON data from the specified path.

    ~~~python
    input_df = spark.readStream.json(shortcut_path)
    ~~~

3. **Defining the Schema**: When working with streaming data, it's crucial to define a schema to understand the structure of the incoming data. In this case, we define the schema as follows:

    ~~~python
    observations_schema = StructType([
        StructField("id", StringType(), True),
        StructField("resourceType", StringType(), True),
        StructField("status", StringType(), True),
        StructField("subject", StructType([  # Nested structure
            StructField("reference", StringType(), True)
        ]), True),
        StructField("category", StructType([  # Nested structure
            StructField("coding", StructType([  # Nested structure
                StructField("code", StringType(), True),
                StructField("display", StringType(), True)
            ]), True)
        ]), True),
        StructField("code", StructType([  # Nested structure
            StructField("coding", StructType([  # Nested structure
                StructField("code", StringType(), True),
                StructField("display", StringType(), True)
            ]), True)
        ]), True),
        StructField("effectiveDateTime", StringType(), True),
        StructField("valueQuantity", StructType([  # Nested structure
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True)
        ]), True)
    ])
    ~~~

4. **Streaming with `availableNow` for Historical Data**: If you're processing historical data, you can use the `trigger(availableNow=True)` option to process all available data in one go. This will handle data that has already arrived, ensuring it’s processed immediately.

    ~~~python
    query = parsed_observations_df.writeStream \
        .outputMode("append") \
        .format("delta") \
        .trigger(availableNow=True) \
        .option("checkpointLocation", "<check_point_location>") \
        .toTable("observations_streaming_silver")  # Save the stream to the Delta table
    ~~~

5. **Switching to Real-Time Data with a 5-second Trigger**: After processing historical data, you can switch to real-time data streaming by using `trigger(processingTime='5 seconds')`. This ensures that new data arriving every 5 seconds is processed immediately, enabling near real-time streaming.

    ~~~python
    query = parsed_observations_df.writeStream \
        .outputMode("append") \
        .format("delta") \
        .trigger(processingTime='5 seconds') \
        .option("checkpointLocation", "<check_point_location>") \
        .toTable("observations_streaming_silver")  # Save the stream to the Delta table
    ~~~

6. **Write the Data to Streaming Target**: Use .toTable to stream insert to target delta table. Ensure checkpointing is configured for fault tolerance.

    ~~~python
    query = parsed_observations_df.writeStream \
        .outputMode("append") \
        .format("delta") \
        .trigger(processingTime='5 seconds') \
        .option("checkpointLocation", "<check_point_location>") \
        .toTable("observations_streaming_silver")  # Save the stream to the Delta table
    ~~~

By using `trigger(availableNow=True)` for historical data and `trigger(processingTime='5 seconds')` for real-time data, you can efficiently process both backlogged data and streaming data with Apache Spark.

**Tip**: Don't forget to stop the stream once you're done testing, as it will continue running indefinitely.

Here is the sample working code:

~~~python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define schema for the expected JSON structure (based on observations data)
observations_schema = StructType([
    StructField("id", StringType(), True),
    StructField("resourceType", StringType(), True),
    StructField("status", StringType(), True),
    StructField("subject", StructType([  # Nested structure
        StructField("reference", StringType(), True)
    ]), True),
    StructField("category", StructType([  # Nested structure
        StructField("coding", StructType([  # Nested structure
            StructField("code", StringType(), True),
            StructField("display", StringType(), True)
        ]), True)
    ]), True),
    StructField("code", StructType([  # Nested structure
        StructField("coding", StructType([  # Nested structure
            StructField("code", StringType(), True),
            StructField("display", StringType(), True)
        ]), True)
    ]), True),
    StructField("effectiveDateTime", StringType(), True),
    StructField("valueQuantity", StructType([  # Nested structure
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True)
    ]), True)
])

# Read JSON data into a streaming DataFrame with schema specified
observations_raw_df = spark.readStream.schema(observations_schema).json(observations_path)

# Print schema to confirm the structure
observations_raw_df.printSchema()

# Parse the raw data and select required fields
parsed_observations_df = observations_raw_df.select(
    "id", "resourceType", "status", "subject.reference", 
    "category.coding.code", "category.coding.display", 
    "effectiveDateTime", "valueQuantity.value", "valueQuantity.unit"
)

# Print the schema of parsed DataFrame
parsed_observations_df.printSchema()

# Write the micro-batch to a Delta table
query = parsed_observations_df.writeStream \
    .outputMode("append") \
    .format("delta") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "abfss://d24c0aa4-e6b2-4571-8e9a-6ae82ebf926d@msit-onelake.dfs.fabric.microsoft.com/9308f974-121a-4491-9d8a-8dde47a9ce9b/Files/streamingcheckpoint/") \
    .toTable("observations_streaming_silver")  # Save the stream to the Delta table

# Await termination (to keep the stream running until all data is processed)
query.awaitTermination()
~~~

## 🎉 Wrapping Up the Exercise: Developing Spark Applications

Congrats on completing this hands-on exercise! 🚀 You've learned the following this chapter:

- **Notebook Development**: You’ve explored how to create and work with Spark notebooks in different interfaces like Fabric UI and VS Code, using standard and high concurrency session.

- **Spark Basics**: You’ve loaded, transformed, and written data in various formats using DataFrames, and you’ve worked with different layers of data processing to prepare it for analytics.

---