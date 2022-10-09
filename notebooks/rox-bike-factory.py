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

# Salvando a query numa variável
df_1 = spark.sql("""SELECT 
                      COUNT(SalesOrderID) TOTAL_PEDIDOS_MAIOR_3_LINHAS
                    FROM 
                      SalesOrderDetail
                    WHERE
                      LineTotal > 3""")

display(df_1)

# COMMAND ----------

# Salvando como tabela para importar no PBI
df_1.write.mode("overwrite").saveAsTable("df_1")

# COMMAND ----------

# MAGIC %md
# MAGIC **2.** Escreva uma query que ligue as tabelas Sales.SalesOrderDetail, Sales.SpecialOfferProduct e Production.Product e retorne os 3 produtos (Name) mais vendidos (pela soma de OrderQty), agrupados pelo número de dias para manufatura (DaysToManufacture).

# COMMAND ----------

df_2 = spark.sql("""SELECT 
                      P.Name AS NOME_PRODUTO, 
                      P.DaysToManufacture AS DIAS_MANUFATURA, 
                      SUM(SOD.OrderQty) AS TOTAL_VENDIDO
                    FROM 
                      SalesOrderDetail SOD
                    INNER JOIN 
                      SpecialOfferProduct SOP
                    ON SOD.SpecialOfferID = SOP.SpecialOfferID
                    INNER JOIN 
                      Product P
                    ON SOP.ProductId = P.ProductID
                    GROUP BY 
                      P.Name, 
                      P.DaysToManufacture
                    ORDER BY
                      SUM(SOD.OrderQty) DESC
                    LIMIT 3""")

display(df_2)

# COMMAND ----------

# Salvando como tabela para importar no PBI
df_2.write.mode("overwrite").saveAsTable("df_2")

# COMMAND ----------

# MAGIC %md
# MAGIC **3.** Escreva uma query ligando as tabelas Person.Person, Sales.Customer e Sales.SalesOrderHeader de forma a obter uma lista de nomes de clientes e uma contagem de pedidos efetuados.

# COMMAND ----------

df_3 = spark.sql("""SELECT 
                      CONCAT(P.FirstName, ' ', P.LastName) AS NOME,
                      COUNT(SOH.CustomerID) AS PEDIDOS_REALIZADOS
                    FROM 
                      Person P
                    INNER JOIN 
                      Customer C
                    ON P.BusinessEntityID = C.CustomerID
                    INNER JOIN 
                      SalesOrderHeader SOH
                    ON SOH.CustomerID = C.CustomerID
                    GROUP BY 
                      CONCAT(P.FirstName, ' ', P.LastName)
                    ORDER BY 
                      COUNT(SOH.CustomerID) DESC""")

display(df_3)

# COMMAND ----------

# Salvando como tabela para importar no PBI
df_3.write.mode("overwrite").saveAsTable("df_3")

# COMMAND ----------

# MAGIC %md
# MAGIC **4.** Escreva uma query usando as tabelas Sales.SalesOrderHeader, Sales.SalesOrderDetail e Production.Product, de forma a obter a soma total de produtos (OrderQty) por ProductID e OrderDate.

# COMMAND ----------

df_4 = spark.sql("""SELECT 
                      P.ProductID AS ID_PRODUTO,
                      CAST(SOH.OrderDate AS DATE) AS DIA_PEDIDO,
                      SUM(SOD.OrderQty) AS TOTAL_PEDIDOS
                    FROM 
                      SalesOrderHeader SOH
                    INNER JOIN
                      SalesOrderDetail SOD
                    ON SOH.SalesOrderID = SOD.SalesOrderID
                    INNER JOIN
                      Product P
                    ON P.ProductID = SOD.ProductID
                    GROUP BY 
                      P.ProductID, 
                      SOH.OrderDate
                    ORDER BY
                      SUM(SOD.OrderQty) DESC""")

display(df_4)

# COMMAND ----------

# Salvando como tabela para importar no PBI
df_4.write.mode("overwrite").saveAsTable("df_4")

# COMMAND ----------

# MAGIC %md
# MAGIC **5.** Escreva uma query mostrando os campos SalesOrderID, OrderDate e TotalDue da tabela Sales.SalesOrderHeader. Obtenha apenas as linhas onde a ordem tenha sido feita durante o mês de setembro/2011 e o total devido esteja acima de 1.000. Ordene pelo total devido decrescente.

# COMMAND ----------

df_5 = spark.sql("""SELECT
                      SalesOrderID AS ORDEM_COMPRA,
                      CAST(OrderDate AS DATE) AS DATA_PEDIDO, -- Timestamp para data
                      CAST(REPLACE(TotalDue, ',' , '.') AS DECIMAL(12,2)) AS TOTAL_DEVIDO -- Formatei a String, mudando vírgula por ponto e convertendo em Decimal para aplicar o WHERE
                    FROM 
                      SalesOrderHeader
                    WHERE 
                      YEAR(OrderDate) = 2011
                      AND
                      CAST(REPLACE(TotalDue, ',' , '.') AS DECIMAL(12,2)) > 1000
                    ORDER BY TOTAL_DEVIDO DESC""")

display(df_5)

# COMMAND ----------

# Salvando como tabela para importar no PBI
df_5.write.mode("overwrite").saveAsTable("df_5")