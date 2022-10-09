# Databricks notebook source
# Passando configurações gerais para os arquivos .csv
config_csv = {
  "delimiter" : ";"
  ,"encode" : "utf-8"
  ,"header" : True
  ,"inferSchema" : True
}

# COMMAND ----------

# Passando o path (diretório) em comum para os arquivos .csv
root_path = "/FileStore/tables/"

# COMMAND ----------

# Criando os dataframes dos csvs carregados no DBFS
df_specialofferproduct = spark.read.options(**config_csv).csv(f'{root_path}Sales_SpecialOfferProduct.csv')
df_product = spark.read.options(**config_csv).csv(f'{root_path}Production_Product.csv')
df_salesorderheader = spark.read.options(**config_csv).csv(f'{root_path}Sales_SalesOrderHeader.csv')
df_customer = spark.read.options(**config_csv).csv(f'{root_path}Sales_Customer.csv')
df_person = spark.read.options(**config_csv).csv(f'{root_path}Person_Person.csv')
df_salesorderdetail = spark.read.options(**config_csv).csv(f'{root_path}Sales_SalesOrderDetail.csv')

# COMMAND ----------

display(df_specialofferproduct)