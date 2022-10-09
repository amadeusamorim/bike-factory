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

# Verificando se a tabela importou corretamente
display(df_specialofferproduct)

# Criando View da tabela Special Offer Product para realizar a análise dos dados em SQL
df_specialofferproduct.createOrReplaceTempView("SpecialOfferProduct")

# COMMAND ----------

# Verificando se a tabela importou corretamente
display(df_product)

# Criando View da tabela Product para realizar a análise dos dados em SQL
df_product.createOrReplaceTempView("Product")

# COMMAND ----------

# Verificando se a tabela importou corretamente
display(df_salesorderheader)

# Criando View da tabela Sales Order Header para realizar a análise dos dados em SQL
df_salesorderheader.createOrReplaceTempView("SalesOrderHeader")

# COMMAND ----------

# Verificando se a tabela importou corretamente
display(df_customer)

# Criando View da tabela Customer para realizar a análise dos dados em SQL
df_customer.createOrReplaceTempView("Customer")

# COMMAND ----------

# Verificando se a tabela importou corretamente
display(df_person)

# Criando View da tabela Person para realizar a análise dos dados em SQL
df_person.createOrReplaceTempView("Person")

# COMMAND ----------

# Verificando se a tabela importou corretamente
display(df_salesorderdetail)

# Criando View da tabela Sales Order Detail para realizar a análise dos dados em SQL
df_salesorderdetail.createOrReplaceTempView("SalesOrderDetail")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Resoluções do exercício | Análise de dados

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC **1.** Escreva uma query que retorna a quantidade de linhas na tabela Sales.SalesOrderDetail pelo campo SalesOrderID, desde que tenham pelo menos três linhas de detalhes.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(SalesOrderID) TOTAL_PEDIDOS_MAIOR_3_LINHAS
# MAGIC FROM 
# MAGIC   SalesOrderDetail
# MAGIC WHERE
# MAGIC   LineTotal > 3;

# COMMAND ----------

# MAGIC %md
# MAGIC **2.** Escreva uma query que ligue as tabelas Sales.SalesOrderDetail, Sales.SpecialOfferProduct e Production.Product e retorne os 3 produtos (Name) mais vendidos (pela soma de OrderQty), agrupados pelo número de dias para manufatura (DaysToManufacture).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   P.Name AS NOME_PRODUTO, 
# MAGIC   P.DaysToManufacture AS DIAS_MANUFATURA, 
# MAGIC   SUM(SOD.OrderQty) AS TOTAL_VENDIDO
# MAGIC FROM 
# MAGIC   SalesOrderDetail SOD
# MAGIC INNER JOIN 
# MAGIC   SpecialOfferProduct SOP
# MAGIC ON SOD.SpecialOfferID = SOP.SpecialOfferID
# MAGIC INNER JOIN 
# MAGIC   Product P
# MAGIC ON SOP.ProductId = P.ProductID
# MAGIC GROUP BY 
# MAGIC   P.Name, 
# MAGIC   P.DaysToManufacture
# MAGIC ORDER BY
# MAGIC   SUM(SOD.OrderQty) DESC
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC **3.** Escreva uma query ligando as tabelas Person.Person, Sales.Customer e Sales.SalesOrderHeader de forma a obter uma lista de nomes de clientes e uma contagem de pedidos efetuados.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   CONCAT(P.FirstName, ' ', P.LastName) AS NOME,
# MAGIC   COUNT(SOH.CustomerID) AS PEDIDOS_REALIZADOS
# MAGIC FROM 
# MAGIC   Person P
# MAGIC INNER JOIN 
# MAGIC   Customer C
# MAGIC ON P.BusinessEntityID = C.CustomerID
# MAGIC INNER JOIN 
# MAGIC   SalesOrderHeader SOH
# MAGIC ON SOH.CustomerID = C.CustomerID
# MAGIC GROUP BY 
# MAGIC   CONCAT(P.FirstName, ' ', P.LastName)
# MAGIC ORDER BY 
# MAGIC   COUNT(SOH.CustomerID) DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **4.** Escreva uma query usando as tabelas Sales.SalesOrderHeader, Sales.SalesOrderDetail e Production.Product, de forma a obter a soma total de produtos (OrderQty) por ProductID e OrderDate.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   P.ProductID AS ID_PRODUTO,
# MAGIC   CAST(SOH.OrderDate AS DATE) AS DIA_PEDIDO,
# MAGIC   SUM(SOD.OrderQty) AS TOTAL_PEDIDOS
# MAGIC FROM 
# MAGIC   SalesOrderHeader SOH
# MAGIC INNER JOIN
# MAGIC   SalesOrderDetail SOD
# MAGIC ON SOH.SalesOrderID = SOD.SalesOrderID
# MAGIC INNER JOIN
# MAGIC   Product P
# MAGIC ON P.ProductID = SOD.ProductID
# MAGIC GROUP BY 
# MAGIC   P.ProductID, 
# MAGIC   SOH.OrderDate
# MAGIC ORDER BY
# MAGIC   SUM(SOD.OrderQty) DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **5.** Escreva uma query mostrando os campos SalesOrderID, OrderDate e TotalDue da tabela Sales.SalesOrderHeader. Obtenha apenas as linhas onde a ordem tenha sido feita durante o mês de setembro/2011 e o total devido esteja acima de 1.000. Ordene pelo total devido decrescente.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SalesOrderID AS ORDEM_COMPRA,
# MAGIC   CAST(OrderDate AS DATE) AS DATA_PEDIDO, -- Timestamp para data
# MAGIC   CAST(REPLACE(TotalDue, ',' , '.') AS DECIMAL(12,2)) AS TOTAL_DEVIDO -- Formatei a String, mudando vírgula por ponto e convertendo em Decimal para aplicar o WHERE
# MAGIC FROM SalesOrderHeader
# MAGIC WHERE 
# MAGIC   YEAR(OrderDate) = 2011
# MAGIC   AND
# MAGIC   CAST(REPLACE(TotalDue, ',' , '.') AS DECIMAL(12,2)) > 1000
# MAGIC ORDER BY TOTAL_DEVIDO DESC;