from datetime import datetime
import pyspark.sql.functions as sparkFunction
import pyspark.sql.types as dataType
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

HOSTNAME = "localhost"
PORT = 1433
USERNAME = "sa"
PASSWORD = "Longhandsome123"
# DB_NAME = "demo"


def createUrl(dbName, hostname=HOSTNAME, port=PORT, username=USERNAME, password=PASSWORD):
    return f"jdbc:sqlserver://{hostname}:{port};database={dbName};user={username};password={password}"


# day number 0
# day number 1
dayNum = 0
fileName = f'../Raw Data/customer_order_{dayNum}.csv'

df = spark.read.csv(fileName, header=True, inferSchema=True)
# trước khi xử lý datetime
df.printSchema()


def convertToDateType(dateString):
    return datetime.strptime(dateString, '%m/%d/%y')


str2Date = sparkFunction.udf(
    f=convertToDateType, returnType=dataType.DateType())
df = df.withColumn('Ship Date', str2Date('Ship Date'))
df = df.withColumn('Order Date', str2Date('Order Date'))
# sau khi xử lý datetime
df.printSchema()

# append to source
df.write.jdbc(createUrl(dbName='SourceSystem'),
              table='customer_order', mode='append')
