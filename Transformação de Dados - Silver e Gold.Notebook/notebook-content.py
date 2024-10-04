# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "77e66095-d6ac-4911-8ffc-6b02fecf216f",
# META       "default_lakehouse_name": "LH_AITOUR",
# META       "default_lakehouse_workspace_id": "273385c4-2068-4ebc-873e-677426cc6069",
# META       "known_lakehouses": [
# META         {
# META           "id": "77e66095-d6ac-4911-8ffc-6b02fecf216f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name
from pyspark.sql.types import *

# Inicializar a sessão Spark
spark = SparkSession.builder.appName("ContosoOutdoors").getOrCreate()

# Criar o esquema para a tabela
orderSchema = StructType([
    StructField("SalesOrderNumber", StringType(), True),
    StructField("SalesOrderLineNumber", IntegerType(), True),
    StructField("OrderDate", DateType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Item", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", FloatType(), True),
    StructField("Tax", FloatType(), True)
])

# Importar todos os arquivos da pasta bronze do lakehouse
try:
    df = spark.read.format("csv").option("header", "true").schema(orderSchema).load("Files/bronze/*.csv")
    
    # Adicionar colunas IsFlagged, CreatedTS e ModifiedTS
    df = df.withColumn("FileName", input_file_name()) \
           .withColumn("IsFlagged", when(col("OrderDate") < '2019-08-01', lit(True)).otherwise(lit(False))) \
           .withColumn("CreatedTS", current_timestamp()) \
           .withColumn("ModifiedTS", current_timestamp())
    
    # Atualizar CustomerName para "Unknown" se CustomerName for nulo ou vazio
    df = df.withColumn("CustomerName", when((col("CustomerName").isNull()) | (col("CustomerName") == ""), lit("Unknown")).otherwise(col("CustomerName")))
    
    # Exibir as primeiras 10 linhas do dataframe para visualizar os dados
    df.show(10)
except Exception as e:
    print(f"Erro ao carregar os dados: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

current_schema = spark.sql("SELECT current_database()").collect()[0][0]
print(f"Esquema atual: {current_schema}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name
from pyspark.sql.types import *
from delta.tables import *

# Inicializar a sessão Spark
spark = SparkSession.builder.appName("ContosoOutdoors").getOrCreate()

# Criar o esquema para a tabela Silver
DeltaTable.createIfNotExists(spark) \
    .tableName("lh_aitour.sales_silver") \
    .addColumn("SalesOrderNumber", StringType()) \
    .addColumn("SalesOrderLineNumber", IntegerType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email", StringType()) \
    .addColumn("Item", StringType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .addColumn("FileName", StringType()) \
    .addColumn("IsFlagged", BooleanType()) \
    .addColumn("CreatedTS", DateType()) \
    .addColumn("ModifiedTS", DateType()) \
    .execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Atualiza registros existentes e insere novos com base em uma condição definida pelas colunas SalesOrderNumber, OrderDate, CustomerName e Item.

from delta.tables import *

# Define o caminho da tabela Delta
deltaTable = DeltaTable.forPath(spark, 'Tables/sales_silver')

# DataFrame com as atualizações
dfUpdates = df

# Realiza a operação de merge (junção) na tabela Delta
deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    # Condição para encontrar correspondências entre as tabelas
    'silver.SalesOrderNumber = updates.SalesOrderNumber and silver.OrderDate = updates.OrderDate and silver.CustomerName = updates.CustomerName and silver.Item = updates.Item'
  ) \
  .whenMatchedUpdate(set =
    {
      # Defina aqui os campos a serem atualizados quando houver correspondência
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      # Insere novos registros quando não houver correspondência
      "SalesOrderNumber": "updates.SalesOrderNumber",
      "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
      "OrderDate": "updates.OrderDate",
      "CustomerName": "updates.CustomerName",
      "Email": "updates.Email",
      "Item": "updates.Item",
      "Quantity": "updates.Quantity",
      "UnitPrice": "updates.UnitPrice",
      "Tax": "updates.Tax",
      "FileName": "updates.FileName",
      "IsFlagged": "updates.IsFlagged",
      "CreatedTS": "updates.CreatedTS",
      "ModifiedTS": "updates.ModifiedTS"
    }
  ) \
  .execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.tables import *

# Crie uma sessão Spark
spark = SparkSession.builder.appName("ContosoOutdoors").getOrCreate()

# Defina o esquema para a tabela dimdate_gold
if not DeltaTable.isDeltaTable(spark, "sales.dimdate_gold"):
    DeltaTable.createIfNotExists(spark) \
        .tableName("lh_aitour.dimdate_gold") \
        .addColumn("OrderDate", DateType()) \
        .addColumn("Day", IntegerType()) \
        .addColumn("Month", IntegerType()) \
        .addColumn("Year", IntegerType()) \
        .addColumn("mmmyyyy", StringType()) \
        .addColumn("yyyymm", StringType()) \
        .execute()
else:
    print("A tabela 'sales.dimdate_gold' já existe como uma tabela Delta.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 from pyspark.sql.functions import col, dayofmonth, month, year, date_format
    
 # Create dataframe for dimDate_gold
    
 dfdimDate_gold = df.dropDuplicates(["OrderDate"]).select(col("OrderDate"), \
         dayofmonth("OrderDate").alias("Day"), \
         month("OrderDate").alias("Month"), \
         year("OrderDate").alias("Year"), \
         date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"), \
         date_format(col("OrderDate"), "yyyyMM").alias("yyyymm"), \
     ).orderBy("OrderDate")

 # Display the first 10 rows of the dataframe to preview your data

 display(dfdimDate_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

# Carrega a tabela Delta existente no caminho especificado
deltaTable = DeltaTable.forPath(spark, 'Tables/dimdate_gold')

# DataFrame com as atualizações
dfUpdates = dfdimDate_gold

# Realiza a operação de merge (junção) entre a tabela Delta e o DataFrame de atualizações
deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.OrderDate = updates.OrderDate'  # Condição de junção baseada na coluna OrderDate
  ) \
  .whenMatchedUpdate(set = 
    {
      # Aqui você pode definir as colunas a serem atualizadas quando houver correspondência
    }
  ) \
  .whenNotMatchedInsert(values = 
    {
      "OrderDate": "updates.OrderDate",  # Insere a coluna OrderDate do DataFrame de atualizações
      "Day": "updates.Day",              # Insere a coluna Day do DataFrame de atualizações
      "Month": "updates.Month",          # Insere a coluna Month do DataFrame de atualizações
      "Year": "updates.Year",            # Insere a coluna Year do DataFrame de atualizações
      "mmmyyyy": "updates.mmmyyyy",      # Insere a coluna mmmyyyy do DataFrame de atualizações
      "yyyymm": "yyyymm"                 # Insere a coluna yyyymm do DataFrame de atualizações
    }
  ) \
  .execute()  # Executa a operação de merge


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *
    
DeltaTable.createIfNotExists(spark) \
    .tableName("LH_AITOUR.dimproduct_gold") \
    .addColumn("ItemName", StringType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("ItemInfo", StringType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 from pyspark.sql.types import *
 from delta.tables import *
    
 # Create customer_gold dimension delta table
 DeltaTable.createIfNotExists(spark) \
     .tableName("LH_AITOUR.dimcustomer_gold") \
     .addColumn("CustomerName", StringType()) \
     .addColumn("Email",  StringType()) \
     .addColumn("First", StringType()) \
     .addColumn("Last", StringType()) \
     .addColumn("CustomerID", LongType()) \
     .execute()

      
 # Create customer_silver dataframe
    
 dfdimCustomer_silver = df.dropDuplicates(["CustomerName","Email"]).select(col("CustomerName"),col("Email")) \
     .withColumn("First",split(col("CustomerName"), " ").getItem(0)) \
     .withColumn("Last",split(col("CustomerName"), " ").getItem(1)) 
    
 # Display the first 10 rows of the dataframe to preview your data

 display(dfdimCustomer_silver.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id, col, lit, max, coalesce, split, when

# Cria o dataframe product_silver
dfdimProduct_silver = df.dropDuplicates(["Item"]).select(col("Item")) \
    .withColumn("ItemName", split(col("Item"), ", ").getItem(0)) \
    .withColumn("ItemInfo", when((split(col("Item"), ", ").getItem(1).isNull()) | (split(col("Item"), ", ").getItem(1) == ""), lit("")).otherwise(split(col("Item"), ", ").getItem(1)))

# Exibe as primeiras 10 linhas do dataframe para pré-visualizar os dados
display(dfdimProduct_silver.head(10))

# Carrega a tabela temporária de produtos
dfdimProduct_temp = spark.read.table("LH_AITOUR.dimProduct_gold")

# Obtém o valor máximo de ItemID
MAXProductID = dfdimProduct_temp.select(coalesce(max(col("ItemID")), lit(0)).alias("MAXItemID")).first()[0]

# Realiza a junção anti entre dfdimProduct_silver e dfdimProduct_temp
dfdimProduct_gold = dfdimProduct_silver.join(dfdimProduct_temp, (dfdimProduct_silver.ItemName == dfdimProduct_temp.ItemName) & (dfdimProduct_silver.ItemInfo == dfdimProduct_temp.ItemInfo), "left_anti")

# Adiciona a coluna ItemID ao dataframe dfdimProduct_gold
dfdimProduct_gold = dfdimProduct_gold.withColumn("ItemID", monotonically_increasing_id() + MAXProductID + 1)

# Exibe as primeiras 10 linhas do dataframe para pré-visualizar os dados
display(dfdimProduct_gold.head(10))

from pyspark.sql.functions import monotonically_increasing_id, col, when, coalesce, max, lit

# Carrega a tabela temporária de clientes
dfdimCustomer_temp = spark.read.table("LH_AITOUR.dimCustomer_gold")

# Obtém o valor máximo de CustomerID
MAXCustomerID = dfdimCustomer_temp.select(coalesce(max(col("CustomerID")), lit(0)).alias("MAXCustomerID")).first()[0]

# Realiza a junção anti entre dfdimCustomer_silver e dfdimCustomer_temp
dfdimCustomer_gold = dfdimCustomer_silver.join(dfdimCustomer_temp, (dfdimCustomer_silver.CustomerName == dfdimCustomer_temp.CustomerName) & (dfdimCustomer_silver.Email == dfdimCustomer_temp.Email), "left_anti")

# Adiciona a coluna CustomerID ao dataframe dfdimCustomer_gold
dfdimCustomer_gold = dfdimCustomer_gold.withColumn("CustomerID", monotonically_increasing_id() + MAXCustomerID + 1)

# Exibe as primeiras 10 linhas do dataframe para pré-visualizar os dados
display(dfdimCustomer_gold.head(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
    
dfdimCustomer_temp = spark.read.table("LH_AITOUR.dimCustomer_gold")
dfdimProduct_temp = spark.read.table("LH_AITOUR.dimProduct_gold")
    
df = df.withColumn("ItemName",split(col("Item"), ", ").getItem(0)) \
    .withColumn("ItemInfo",when((split(col("Item"), ", ").getItem(1).isNull() | (split(col("Item"), ", ").getItem(1)=="")),lit("")).otherwise(split(col("Item"), ", ").getItem(1))) \
    
    
# Create Sales_gold dataframe
    
dffactSales_gold = df.alias("df1").join(dfdimCustomer_temp.alias("df2"),(df.CustomerName == dfdimCustomer_temp.CustomerName) & (df.Email == dfdimCustomer_temp.Email), "left") \
        .join(dfdimProduct_temp.alias("df3"),(df.ItemName == dfdimProduct_temp.ItemName) & (df.ItemInfo == dfdimProduct_temp.ItemInfo), "left") \
    .select(col("df2.CustomerID") \
        , col("df3.ItemID") \
        , col("df1.OrderDate") \
        , col("df1.Quantity") \
        , col("df1.UnitPrice") \
        , col("df1.Tax") \
    ).orderBy(col("df1.OrderDate"), col("df2.CustomerID"), col("df3.ItemID"))
    
# Display the first 10 rows of the dataframe to preview your data
    
display(dffactSales_gold.head(10))




deltaTable = DeltaTable.forPath(spark, 'Tables/dimcustomer_gold')
    
dfUpdates = dfdimCustomer_gold
    
deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.CustomerName = updates.CustomerName AND silver.Email = updates.Email'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "CustomerName": "updates.CustomerName",
      "Email": "updates.Email",
      "First": "updates.First",
      "Last": "updates.Last",
      "CustomerID": "updates.CustomerID"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/dimproduct_gold')
            
dfUpdates = dfdimProduct_gold
            
deltaTable.alias('silver') \
  .merge(
        dfUpdates.alias('updates'),
        'silver.ItemName = updates.ItemName AND silver.ItemInfo = updates.ItemInfo'
        ) \
        .whenMatchedUpdate(set =
        {
               
        }
        ) \
        .whenNotMatchedInsert(values =
         {
          "ItemName": "updates.ItemName",
          "ItemInfo": "updates.ItemInfo",
          "ItemID": "updates.ItemID"
          }
          ) \
          .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
