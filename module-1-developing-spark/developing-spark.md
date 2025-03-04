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

---