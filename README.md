# PROJETO FÁBRICA DE BICICLETAS


## 📌 O PROBLEMA
O presente problema se refere aos dados de uma empresa que produz bicicletas e o objetivo deste desafio é cumprir os seguintes aspectos:
* Fazer a modelagem conceitual dos dados disponibilizados;
*	Criar a infraestrutura necessária em nuvem;
*	Criar de todos os artefatos necessários para carregar os arquivos para o banco criado;
*	Desenvolvimento de SCRIPT para análise de dados;
*	(opcional) Criar um relatório em qualquer ferramenta de visualização de dados.

Os seguintes arquivos devem ser importados (ETL) para o banco de dados de sua escolha: 
*	Sales.SpecialOfferProduct.csv
*	Production.Product.csv
*	Sales.SalesOrderHeader.csv
*	Sales.Customer.csv
*	Person.Person.csv
*	Sales.SalesOrderDetail.csv

### 📈 DADOS PARA ANÁLISE

Com base na solução implantada responda aos seguintes questionamentos:
*	Escreva uma query que retorna a quantidade de linhas na tabela Sales.SalesOrderDetail pelo campo SalesOrderID, desde que tenham pelo menos três linhas de detalhes.
*	Escreva uma query que ligue as tabelas Sales.SalesOrderDetail, Sales.SpecialOfferProduct e Production.Product e retorne os 3 produtos (Name) mais vendidos (pela soma de OrderQty), agrupados pelo número de dias para manufatura (DaysToManufacture).
*	Escreva uma query ligando as tabelas Person.Person, Sales.Customer e Sales.SalesOrderHeader de forma a obter uma lista de nomes de clientes e uma contagem de pedidos efetuados.
*	Escreva uma query usando as tabelas Sales.SalesOrderHeader, Sales.SalesOrderDetail e Production.Product, de forma a obter a soma total de produtos (OrderQty) por ProductID e OrderDate.
*	Escreva uma query mostrando os campos SalesOrderID, OrderDate e TotalDue da tabela Sales.SalesOrderHeader. Obtenha apenas as linhas onde a ordem tenha sido feita durante o mês de setembro/2011 e o total devido esteja acima de 1.000. Ordene pelo total devido decrescente.



## ✔️ RESOLUÇÃO

### 🔶 ELABORAÇÃO DO MODELO CONCEITUAL
Foi levado em consideração os dados que tinham correlacionamento/correspondência de uma tabela para outra. Um importante fator na modelagem dos dados em questão é de que mesmo que não haja uma oferta especial, a tabela *SpecialOfferProduct* se fará relevante para conectar-se com a tabela *Product* dentro da regra de negócio apresentada.

Abaixo o demonstrativo da **modelagem realizada**:

[![QrA8nn.md.png](https://iili.io/QrA8nn.md.png)](https://freeimage.host/i/QrA8nn)




### 📁 PASTAS E ARQUIVOS

Dentro do respectivo repositório, temos algumas pastas e arquivos e abaixo informo com mais detalhes suas funções dentro do projeto.

* **raw-folder**: Pasta que constará os **arquivos brutos do projeto**, ou seja, os arquivos em formato .csv que serão levados ao Databricks por meio de uma ingestão via DBFS, formatados em dataframes e preparados para suas respectivas análises. 
* **modelagem-conceitual**: Pasta que constará o arquivo editável e a imagem da modelagem conceitual da fábrica de bicicletas, mostrando as ligações entre as tabelas apresentadas.

### 📊 Visualizações


### 🔧 Ferramentas utilizadas
- ``BR Modelo``
- ``Azure Databricks``
- ``Spark``

