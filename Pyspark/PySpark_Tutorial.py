# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df_json = spark.read.format('json').option('inferSchema',True)\
                    .option('header',True)\
                    .option('multiLine',False)\
                    .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # DDL Schema

# COMMAND ----------

my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 

                ''' 

# COMMAND ----------

df = spark.read.format('csv')\
         .schema(my_ddl_schema)\
         .option('header',True)\
         .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # StructType Schema 

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import *  

# COMMAND ----------

my_strct_schema = StructType([ StructField('Item_Identifier',StringType(),True), 
                               StructField('Item_Weight',StringType(),True), 
                               StructField('Item_Fat_Content',StringType(),True), 
                               StructField('Item_Visibility',StringType(),True), 
                               StructField('Item_MRP',StringType(),True), 
                               StructField('Outlet_Identifier',StringType(),True), 
                               StructField('Outlet_Establishment_Year',StringType(),True), 
                               StructField('Outlet_Size',StringType(),True), 
                               StructField('Outlet_Location_Type',StringType(),True), 
                               StructField('Outlet_Type',StringType(),True), 
                               StructField('Item_Outlet_Sales',StringType(),True)

])

# COMMAND ----------

df = spark.read.format('csv')\
               .schema(my_strct_schema)\
               .option('header',True)\
               .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select

# COMMAND ----------

df.select("Item_Identifier","Item_Weight","Item_Fat_Content").display()

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alias

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Filters

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight') < 10)).display()

# COMMAND ----------

df.filter((col('Outlet_Location_Type').isin('Tier 1','Tier 2')) 
          & (col('Outlet_Size').isNull())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #WithColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #WithColumn

# COMMAND ----------

df.withColumn('flag',lit("new")).display()

# COMMAND ----------

df.withColumn('Multiply',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),'Low Fat','LF'))\
  .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),'Regular','Reg'))\
  .display()    

# COMMAND ----------

# MAGIC %md
# MAGIC # Type Casting

# COMMAND ----------

df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))\
   .withColumn('Item_Visibility', col('Item_Visibility').cast(StringType()))\
    .printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sorting

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending = [0,0]).display()

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

df.drop('Item_Type','Item_visibility').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop Duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

df.dropDuplicates(subset=['Item_Type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Union and UnionByName

# COMMAND ----------

data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [('kad','1',),
        ('sid','2',)]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

df1.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # String Functions

# COMMAND ----------

from pyspark.sql.functions import *
df.select(upper('Item_Type').alias('Upper_Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Function

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Date

# COMMAND ----------

df = df.withColumn('Current_Date',current_date())

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADD DATE

# COMMAND ----------

df = df.withColumn('week_after',date_add('current_date',7))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SUB DATE

# COMMAND ----------

df = df.withColumn('subdate',date_sub('week_after',7))

df.display()

# COMMAND ----------

df = df.withColumn('subdate',date_add('current_date',-7))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Date Diff

# COMMAND ----------

df = df.withColumn('datediff',date_diff('current_date','week_after'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Format

# COMMAND ----------

df = df.withColumn('week_after',date_format('week_after','dd-MM-yyyy'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Handling Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dropping Nulls

# COMMAND ----------

df = df.dropna('all')
df.display()

# COMMAND ----------

df = df.dropna('any')
df.display()

# COMMAND ----------

df = df.dropna(subset=['Outlet_Size'])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filling Nulls

# COMMAND ----------

df = df.fillna('NotAvailable')
df.display()

# COMMAND ----------

df.fillna('NotAvailable',subset=['Item_Weight']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Splitting and Indexing

# COMMAND ----------

df = df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df.display()

# COMMAND ----------

df = df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1])
df.display()

# COMMAND ----------

df.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

df.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # GroupBy and Aggregations

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Collect_List

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PIVOT

# COMMAND ----------

df.select('Item_Type','Outlet_Size','Item_MRP').display()

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## When-Otherwise

# COMMAND ----------

df = df.withColumn('veg_flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))
df.display()

# COMMAND ----------

df.withColumn('expense_flag',when((col('veg_flag')== 'Veg') & (col('Item_MRP')>100),'Expensive')\
                            .when((col('veg_flag')== 'Veg') & (col('Item_MRP')<100),'InExpensive')\
                            .otherwise('Non_Veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # JOINS

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## INNER JOIN

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## RIGHT JOIN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## LEFT JOIN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ANTI JOIN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # WINDOW FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## ROW_NUMBER

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, row_number, dense_rank

# COMMAND ----------

df.withColumn('rowno',row_number().over(Window().orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # RANK and dense_rank

# COMMAND ----------

df = df.withColumn(
    'rankno',
    rank().over(Window().orderBy(col('Item_Identifier').desc()))
).withColumn(
    'drankno',
    dense_rank().over(Window().orderBy(col('Item_Identifier').desc()))
)

display(df)

# COMMAND ----------

df.withColumn('dum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Cumulative Sum

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # USER DEFINED FUNCTION(UDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP-1

# COMMAND ----------

def my_func(x):
    return x*x 

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP-2

# COMMAND ----------

my_udf = udf(my_func)

# COMMAND ----------

df.withColumn('mynewcol',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA WRITING

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV

# COMMAND ----------

