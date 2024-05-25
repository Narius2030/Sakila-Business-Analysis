# Data-Mining-with-ApacheHive

## Introduction
Data set context: Sakila contains data about movie rental and payment transactions. In addition, it also contains information about movies and customers.

Goal: Build a data warehouse with Apache Hive, extract, transform, and store (ETL) in Dimension and Fact tables. Serves for analyzing operations, helping to improve business strategies to bring profits to businesses

### 1. Customer Segmentation

Customer segmentation: aims to group customers according to certain behavioral characteristics, forming data clusters. From there, marketing campaigns or incentives will easily reach the appropriate customer groups
- Use clustering model according to KMean algorithm: important attributes in this model are recency, frequency and currency
- The performance measures of the model are SSE, sum of squared distances to cluster centers and Silhouette Coefficient method. From there, you can choose the best number of clusters so that the data is clustered more clearly

### 2. Film Inventory Analysis

The goal is to track the quantity of inventory and revenue of each film in the film warehouse. So that, there will be strategies to adjust the process of importing and exporting inventory accordingly

## Connect Apache Hive on Python
Edit the core-site.xml file in Hadoop, add the proxy configuration section for the user and close the file
> **Note:** start hiveserver2 before connect
```xml
<property>
    <name>hadoop.proxyuser.<username>.hosts</name>
    <value>*</value>
</property>
<property>
    <name>hadoop.proxyuser.<username>.groups</name>
    <value>*</value>
</property>
```

## Schemas

* Work-flow:

![Hive_App-Work-flow](https://github.com/Narius2030/Data-Mining-with-ApacheHive/assets/94912102/d6051d77-679b-4405-8471-5b4b80183381)


* Galaxy Schema of Data Warehouse:

![Hive_App-Hive Architecture](https://github.com/Narius2030/Data-Mining-with-ApacheHive/assets/94912102/81dc04fd-2387-4cce-962f-a5868adc8cab)

