# Databricks notebook source
# MAGIC %md
# MAGIC ###Load Packages

# COMMAND ----------

import pyodbc 
import ast
import json
import os
import yaml
import logging
import datetime
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.config("spark.sql.execution.arrow.pysaprk.enabled","true").appName('Dataframe').getOrCreate()

# COMMAND ----------

import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np 
import warnings
import logging
import datetime
warnings.filterwarnings("ignore")

# COMMAND ----------

model = SentenceTransformer('all-MiniLM-L6-v2')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reset Results Table

# COMMAND ----------

# dbutils.widgets.text("catalog","<catalog name>")
# dbutils.widgets.text("schema","<schema name>")
# dbutils.widgets.text("columnScoreResults","<column score table>")
# dbutils.widgets.text("tableScoreResults","<table score table>")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
columnScoreResults = dbutils.widgets.get("columnScoreResults")
tableScoreResults = dbutils.widgets.get("tableScoreResults")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $catalog.$schema.$columnScoreResults;
# MAGIC -- DROP TABLE IF EXISTS $catalog.$schema.$tableScoreResults;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE $catalog.$schema.$columnScoreResults
# MAGIC (relationType STRING, sourceCatalogName STRING, sourceSchemaName STRING, sourceTableName STRING, sourceColumnName STRING, targetCatalogName STRING, targetSchemaName STRING, targetTableName STRING, targetColumnName STRING, SemSimlScore DOUBLE);
# MAGIC CREATE OR REPLACE TABLE $catalog.$schema.$tableScoreResults
# MAGIC (relationType STRING, sourceCatalogName STRING, sourceSchemaName STRING, sourceTableName STRING, targetCatalogName STRING, targetSchemaName STRING, targetTableName STRING, SemSimlScore DOUBLE);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input: provide list of tables

# COMMAND ----------

target_table_list = [f'{catalog}.{schema}.toystore01', f'{catalog}.{schema}.toystore02', f'{catalog}.{schema}.coffeebrewer01', f'{catalog}.{schema}.coffeeshop01']
print(target_table_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Semantic Similarity Function
# MAGIC * Apply SentenceTransformer model to create embedding vectors
# MAGIC * Utilize cosine similarity to determine similarity score 
# MAGIC * This function also insert similarity scores into the results table

# COMMAND ----------

def sem_siml_score(df1,df2):
    data_collect_01 = df1.collect()
    data_collect_02 = df2.collect()
    total_score = []

    # Looping through each row of the dataframe
    for row_01 in data_collect_01:
        for row_02 in data_collect_02:
            embeddings_01 = model.encode(row_01["desc"])
            embeddings_02 = model.encode(row_02["desc"])
            sem_array = cosine_similarity([embeddings_01], [embeddings_02])
            sem_score = 0 if sem_array[0][0] < 0 else np.round((sem_array[0][0])*100)
            total_score.append(sem_score)
            relation_type = "columns"
            print(relation_type, " ", row_01["catalog_name"], " ", row_01["schema_name"], " ", row_01["table_name"], " ", row_01["col_name"], " ", row_02["catalog_name"], " ", row_02["schema_name"], " ", row_02["table_name"], " ", row_02["col_name"], sem_score)
            spark.sql("""
            INSERT INTO {}.{}.{}
            VALUES('{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
            """.format(catalog, schema, columnScoreResults,relation_type, row_01["catalog_name"], row_01["schema_name"], row_01["table_name"], row_01["col_name"], row_02["catalog_name"], row_02["schema_name"], row_02["table_name"], row_02["col_name"], sem_score))

    table_sem_score = np.round(np.mean(total_score))
    relation_type = "tables"

    spark.sql("""
    INSERT INTO {}.{}.{}
    VALUES('{}','{}', '{}', '{}','{}', '{}', '{}','{}')
    """.format(catalog, schema, tableScoreResults, relation_type, row_01["catalog_name"], row_01["schema_name"], row_01["table_name"], row_02["catalog_name"], row_02["schema_name"], row_02["table_name"], table_sem_score))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execution
# MAGIC * The first cell will create a combination list from the input tables
# MAGIC * The second cell will create the input dataframes and run the similarity function

# COMMAND ----------

# from itertools import combinations, permutations
# table_comb = [list(map(str, comb)) for comb in permutations(target_table_list, 2)]
# print(table_comb)

# COMMAND ----------

from itertools import combinations
table_comb = [list(map(str, comb)) for comb in combinations(target_table_list, 2)]
print(table_comb)

# COMMAND ----------

# Filter any field that is not required (like date/timestamp in a fact table)
filter_fields = ['date'] 

# Iterate through the combinations
for table in table_comb:
        table_name_01 = table[0]
        df = spark.sql(f"DESCRIBE TABLE {table_name_01}")
        df1 = (df.selectExpr("col_name", "comment AS desc")
                .filter(~col('col_name').isin(filter_fields))
                .withColumn("catalog_name", lit(table_name_01.split(".")[0]))
                .withColumn("schema_name", lit(table_name_01.split(".")[1]))
                .withColumn("table_name", lit(table_name_01.split(".")[2]))        
                )

        table_name_02 = table[1]
        df = spark.sql(f"DESCRIBE TABLE {table_name_02}")
        df2 = (df.selectExpr("col_name", "comment AS desc")
                .filter(~col('col_name').isin(filter_fields))
                .withColumn("catalog_name", lit(table_name_02.split(".")[0]))
                .withColumn("schema_name", lit(table_name_02.split(".")[1]))
                .withColumn("table_name", lit(table_name_02.split(".")[2])) 
        )      

        sem_siml_score(df1,df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results
# MAGIC * kgcolscore table provides score between each column
# MAGIC * kgtablescore table shows the score between tables 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM $catalog.$schema.$columnScoreResults LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM $catalog.$schema.$tableScoreResults LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Neo4j

# COMMAND ----------

# MAGIC %md
# MAGIC * Connect to Azure Instance

# COMMAND ----------

from pyspark.sql import SparkSession

url = dbutils.secrets.get(scope="neo4j-secrets", key="neo4jurl")
password = dbutils.secrets.get(scope="neo4j-secrets", key="neo4jpassword")
username = "neo4j"

spark = (
    SparkSession.builder.config("neo4j.url", url)
    .config("neo4j.authentication.basic.username", username)
    .config("neo4j.authentication.basic.password", password)
    .getOrCreate()
)

# COMMAND ----------

tablesDF = (spark.table(f'{catalog}.{schema}.{tableScoreResults}')
            .withColumn("relationID", expr(" sha2(concat(sourceCatalogName,sourceSchemaName,sourceTableName,targetCatalogName,targetSchemaName,targetTableName),256) ")))
tablesDF.show(10)

# COMMAND ----------

(
    tablesDF.write
    .mode("Overwrite")
    .format("org.neo4j.spark.DataSource")
    .option("relationship", "Similarity")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.labels", ":Source")
    .option("relationship.source.node.properties", "sourceCatalogName,sourceSchemaName,sourceTableName,relationID:id")
    .option("relationship.target.save.mode", "Overwrite")
    .option("relationship.target.labels", ":Target")
    .option("relationship.target.node.properties", "targetCatalogName,targetSchemaName,targetTableName")
    .option("relationship.properties", "SemSimlScore")
    .save()
)
