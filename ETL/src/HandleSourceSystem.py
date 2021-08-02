from datetime import datetime
import pymssql
from pyspark.sql import SparkSession
import pyspark.sql.functions as sparkFunction
import pyspark.sql.types as dataType
spark = SparkSession.builder.getOrCreate()

# configure something
HOSTNAME = 'localhost'
PORT = 1433
USERNAME = 'sa'
PASSWORD = 'Longhandsome123'
SOURCE = 'SourceSystem'
WAREHOUSE = 'DataStore'
METADB = 'MetaDatabase'

def createUrl(dbName, hostname=HOSTNAME, port=PORT, username=USERNAME, password=PASSWORD):
    """create an url in order to connect to db

    Args:
        dbName (str): name of database
        hostname (str, optional): hostname. Defaults to HOSTNAME.
        port (int, optional): port. Defaults to PORT.
        username (str, optional): username of db. Defaults to USERNAME.
        password (password, optional): password of db. Defaults to PASSWORD.

    Returns:
        str: url
    """
    return f'jdbc:sqlserver://{hostname}:{port};database={dbName};user={username};password={password}'

def shape(df):
    """print out the shape of a dataframe

    Args:
        df (dataframe): dataframe in pyspark
    """
    print(f'shape: ({df.count()}, {len(df.columns)})')

def getSourceFromFile(fileNumber):
    """get data from .csv file for test purpose

    Args:
        fileNumber (int): the file number (0 -> 4)

    Returns:
        dataframe: a dataframe in pyspark
    """
    dayNum = fileNumber
    fileName = f'../Raw Data/customer_order_{dayNum}.csv'

    df = spark.read.csv(fileName, header=True, inferSchema=True)

    def convertToDateType(dateString):
        return datetime.strptime(dateString, '%m/%d/%y')

    str2Date = sparkFunction.udf(f=convertToDateType, returnType=dataType.DateType())
    df = df.withColumn('Ship Date', str2Date('Ship Date'))
    df = df.withColumn('Order Date', str2Date('Order Date'))
    return df

# connect and query in werehouse
conn = pymssql.connect(server=HOSTNAME, user=USERNAME, password=PASSWORD, database=WAREHOUSE)
conn.autocommit(True)
cur = conn.cursor()

# connect and query in metaDB
conn1 = pymssql.connect(server=HOSTNAME, user=USERNAME, password=PASSWORD, database=METADB)
conn1.autocommit(True)
cur1 = conn1.cursor()

class ETL:
    def __init__(self) -> None:
        # setup the row id for extracting
        # oldRowID: the latest rowID that's been extracted
        # newRowID: the new on that the ETL-er is gonna extract from
        self.oldRowID = 0
        self.newRowID = 0
        self._logToMetaDB(0, 'start')

    def _getSource(self):
        """get source data from database

        Returns:
            dataframe: a dataframe in pyspark
        """
        # get max rowID of the previous extraction and assign to oldRowID
        queryMeta = f'''(
            SELECT a1.rowID
            FROM History a1
            WHERE NOT EXISTS (
                SELECT *
                FROM History a2
                WHERE a2.rowID > a1.rowID
            )
        ) T'''
        df_meta = spark.read.jdbc(url=createUrl(dbName=METADB), table=queryMeta)

        try:
            self.oldRowID = df_meta.collect()[0][0]
        except:
            self.oldRowID = 0

        # get all of records whose rowID > oldRowID
        print(f'ETL from {self.oldRowID + 1}')
        querySource = f'''(
            SELECT * 
            FROM customer_order
            WHERE [Row ID] > {self.oldRowID}
        ) T'''

        return spark.read.jdbc(url=createUrl(dbName=SOURCE), table=querySource)

    def _logToMetaDB(self, rowID, detail):
        """logs activities to meta database

        Args:
            rowID (int): the latest row ID that's has been extracted
            detail (str): the description for the action
        """
        query = None
        if rowID is None:
            query = f'''
                INSERT INTO History (detail) VALUES ('{detail}')
            '''
        else:
            query = f'''
                INSERT INTO History (rowID, detail) VALUES ({rowID}, '{detail}')
            '''
        try:
            cur1.execute(query)
        except:
            print(f'error in _logToMetaDB: rowID: {rowID}, detail: {detail}')

    def _getNewData(self, sourceDf, targetData, cols, idCols, getUpdate=True):
        """get new data by: source left join data warehouse table

        Args:
            sourceDf (dataframe): a df that contains source data
            targetData (dataframe): a dataframe that contains data warehouse table
            cols (list): a list of column name that can be conflict when we join 2 dataframes
            idCols (str or list): the key of table (in practice)
            getUpdate (bool, optional): True: I want the update data of the table, False: I don't want it. Defaults to True.

        Returns:
            newData: the new data that's been extracted and transformed from source
            updateDate: the updating data that's been extracted and transformed from source
        """
        # cols can be conflict --> have to rename
        for col in cols:
            targetData = targetData.withColumnRenamed(col, f'{col}_dim')

        # source left join dim and return new data
        joinData = sourceDf.join(targetData, on=idCols, how='left')

        updateData = None
        # if we're not extracting dimDate
        if getUpdate:
            updateData = joinData.where(joinData[f'{cols[0]}_dim'].isNotNull())
            # get the max shipDate and min orderDate and update
            rowMax = sparkFunction.greatest(updateData['ID Expiry Date'], updateData['ID Expiry Date_dim'])
            rowMin = sparkFunction.least(updateData['ID Manufacturing Date'], updateData['ID Manufacturing Date_dim'])
            updateData = updateData.withColumn('ID Manufacturing Date', rowMin)
            updateData = updateData.withColumn('ID Expiry Date', rowMax)
            updateData = updateData.drop(*[f'{col}_dim' for col in cols])
            if updateData.count() == 0:
                updateData = None

        newData = joinData.where(joinData[f'{cols[0]}_dim'].isNull())
        # drop all the null value in the right side
        newData = newData.drop(*[f'{col}_dim' for col in cols])

        return newData, updateData

    def _getSourceSystemDate(self, df):
        """get date data from source system (no duplication)

        Args:
            df (dataframe): source dataframe

        Returns:
            dataframe: a df that contains dates that's been extracted from source df
        """
        shipDate = df[['Ship Date']]
        orderDate = df[['Order Date']]
        orderDate = orderDate.withColumnRenamed('Order Date', 'Ship Date')

        date = shipDate.union(orderDate)
        date = date.withColumnRenamed('Ship Date', 'date')

        # drop duplicate and make id col
        date = date.dropDuplicates()
        date = date.withColumn('idDate', sparkFunction.date_format(date['date'], 'yyyyMMdd'))
        date = date.withColumn('idDate', date['idDate'].cast('int'))

        return date

    def ETLDate(self, df):
        """do the ETL for date

        Args:
            df (dataframe): source df
        """
        sourceDate = self._getSourceSystemDate(df)
        dimDate = spark.read.jdbc(url=createUrl(dbName=WAREHOUSE), table='dimDate')

        if dimDate.count() == 0:
            numOfNewDate = sourceDate.count()
            if numOfNewDate != 0:
                sourceDate.write.jdbc(url=createUrl(dbName=WAREHOUSE), table='dimDate', mode='append')
                # self.newRowID = self.oldRowID + numOfNewDate
                stt = f'Write {numOfNewDate} dates into db'
                print(stt)
                self._logToMetaDB(self.newRowID, stt)
            else:
                print('nothing new in source date')
        else:
            newDate, _ = self._getNewData(sourceDate, dimDate, ['date'], 'idDate', getUpdate=False)
            numOfNewDate = newDate.count()
            if numOfNewDate != 0:
                # self.newRowID = self.oldRowID + numOfNewDate
                newDate.write.jdbc(url=createUrl(dbName=WAREHOUSE), table='dimDate', mode='append')
                stt = f'Write {numOfNewDate} dates into db'
                print(stt)
                self._logToMetaDB(self.newRowID, stt)

    def _getSourceSystemCustomer(self, df):
        """get customer data from source system

        Args:
            df (dataframe): source df

        Returns:
            dataframe: a df that contains info of customers in source (no duplication)
        """
        sourceCustomer = df.groupBy(['Customer ID', 'Customer Name', 'Segment', 'Country', 'City', 'State', 'Postal Code', 'Region']).agg({'Order Date': 'min', 'Ship Date': 'max'})
        sourceCustomer = sourceCustomer.withColumnRenamed('max(Ship Date)', 'ID Expiry Date')
        sourceCustomer = sourceCustomer.withColumnRenamed('min(Order Date)', 'ID Manufacturing Date')
        sourceCustomer = sourceCustomer.withColumn('ID Manufacturing Date', 
                                                    sparkFunction.date_format(sourceCustomer['ID Manufacturing Date'], 'yyyyMMdd'))
        sourceCustomer = sourceCustomer.withColumn('ID Manufacturing Date', 
                                                    sourceCustomer['ID Manufacturing Date'].cast('int'))
        sourceCustomer = sourceCustomer.withColumn('ID Expiry Date', 
                                                    sparkFunction.date_format(sourceCustomer['ID Expiry Date'], 'yyyyMMdd'))
        sourceCustomer = sourceCustomer.withColumn('ID Expiry Date', 
                                                    sourceCustomer['ID Expiry Date'].cast('int'))
        return sourceCustomer

    def _handleUpdatedDataCustomer(self, df):
        """update data of customer in data warehouse: I overwrite them with the given ID

        Args:
            df (dataframe): a df that contains updating data of customer
        """
        l = df.collect()
        for i in l:
            query = f'''
                MERGE INTO dimCustomer AS t
                USING 
                    (SELECT 
                        [Customer ID] = '{i['Customer ID']}', 
                        [Customer Name] = '{i['Customer Name'].replace("'", '"')}',
                        [Segment] = '{i['Segment'].replace("'", '"')}',
                        [Country] = '{i['Country'].replace("'", '"')}',
                        [City] = '{i['City'].replace("'", '"')}',
                        [State] = '{i['State'].replace("'", '"')}',
                        [Postal Code] = {i['Postal Code']},
                        [Region] = '{i['Region'].replace("'", '"')}',
                        [ID Manufacturing Date] = {i['ID Manufacturing Date']},
                        [ID Expiry Date] = {i['ID Expiry Date']},
                        [idCustomer] = {i['idCustomer']}) AS s
                ON t.idCustomer = s.idCustomer
                WHEN MATCHED THEN UPDATE SET 
                        [Customer ID] = s.[Customer ID],
                        [Customer Name] = s.[Customer Name],
                        [Segment] = s.[Segment],
                        [Country] = s.[Country],
                        [City] = s.[City],
                        [State] = s.[State],
                        [Postal Code] = s.[Postal Code],
                        [Region] = s.[Region],
                        [ID Manufacturing Date] =  s.[ID Manufacturing Date],
                        [ID Expiry Date] =  s.[ID Expiry Date];
            '''
            try:
                cur.execute(query)
                # conn.commit()
            except:
                print(f'error in handleUpdatedDataCustomer, update failed {len(l)} customers')
                return
        stt = f'Updated {len(l)} customers'
        print(stt)
        self._logToMetaDB(self.newRowID, stt)

    def ETLCustomer(self, df):
        """do the ETL job for customer

        Args:
            df (dataframe): source data
        """
        sourceCustomer = self._getSourceSystemCustomer(df)
        dimCustomer = spark.read.jdbc(url=createUrl(dbName=WAREHOUSE), table='dimCustomer')

        if dimCustomer.count() == 0:
            numOfNewCustomer = sourceCustomer.count()
            if numOfNewCustomer != 0:
                sourceCustomer.write.jdbc(url=createUrl(dbName=WAREHOUSE), table='dimCustomer', mode='append')
                stt = f'Write {numOfNewCustomer} customer into db'
                print(stt)
                self._logToMetaDB(self.newRowID, stt)
            else:
                print('nothing new in source customer')
        else:
            newCustomer, updateCustomer = self._getNewData(
                sourceCustomer, 
                dimCustomer, 
                ['ID Manufacturing Date', 'ID Expiry Date'], 
                ['Customer ID', 'Customer Name', 'Segment', 'Country', 'City', 'State', 'Postal Code', 'Region'])
            if updateCustomer is not None:
                self._handleUpdatedDataCustomer(updateCustomer)
            numOfNewCustomer = newCustomer.count()
            if numOfNewCustomer != 0:
                newCustomer = newCustomer.drop('idCustomer')
                newCustomer.write.jdbc(url=createUrl(dbName=WAREHOUSE), table='dimCustomer', mode='append')
                stt = f'Write {numOfNewCustomer} customer into db'
                print(stt)
                self._logToMetaDB(self.newRowID, stt)
            else:
                print('nothing new in source customer')

    def _getSourceSystemProduct(self, df):
        """get data of product in source system

        Args:
            df (dataframe): source df

        Returns:
            dataframe: a df that contains data of product that's been extracted from source system (no duplication)
        """
        sourceProduct = df.groupBy(['Product ID', 'Product Name', 'Category', 'Sub-Category']).agg({'Order Date': 'min', 'Ship Date': 'max'})
        sourceProduct = sourceProduct.withColumnRenamed('max(Ship Date)', 'ID Expiry Date')
        sourceProduct = sourceProduct.withColumnRenamed('min(Order Date)', 'ID Manufacturing Date')
        sourceProduct = sourceProduct.withColumn('ID Manufacturing Date', 
                                                sparkFunction.date_format(sourceProduct['ID Manufacturing Date'], 'yyyyMMdd'))
        sourceProduct = sourceProduct.withColumn('ID Manufacturing Date', 
                                                sourceProduct['ID Manufacturing Date'].cast('int'))
        sourceProduct = sourceProduct.withColumn('ID Expiry Date', 
                                                sparkFunction.date_format(sourceProduct['ID Expiry Date'], 'yyyyMMdd'))
        sourceProduct = sourceProduct.withColumn('ID Expiry Date', 
                                                sourceProduct['ID Expiry Date'].cast('int'))
        return sourceProduct

    def _handleUpdatedDataProduct(self, df):
        """update data of product in data warehouse: I overwrite them with the given ID

        Args:
            df (dataframe): a df that contains updating data of product
        """
        l = df.collect()
        for i in l:
            query = f'''
                MERGE INTO dimProduct AS t
                USING 
                    (SELECT 
                        [idProduct] = {i['idProduct']},
                        [Product ID] = '{i['Product ID']}',
                        [Product Name] = '{i['Product Name'].replace("'", '"')}',
                        [Category] = '{i['Category'].replace("'", '"')}',
                        [Sub-Category] = '{i['Sub-Category'].replace("'", '"')}',
                        [ID Manufacturing Date] = {i['ID Manufacturing Date']},
                        [ID Expiry Date] = {i['ID Expiry Date']}) AS s
                ON t.idProduct = s.idProduct
                WHEN MATCHED THEN UPDATE SET 
                        [Product ID] = s.[Product ID],
                        [Product Name] = s.[Product Name],
                        [Category] = s.[Category],
                        [Sub-Category] = s.[Sub-Category],
                        [ID Manufacturing Date] = s.[ID Manufacturing Date],
                        [ID Expiry Date] = s.[ID Expiry Date];
            '''
            try:
                cur.execute(query)
                # conn.commit()
            except:
                print(f'error in handleUpdatedDataProduct, update failed {len(l)} products')
                return
        stt = f'Updated {len(l)} products'
        print(stt)
        self._logToMetaDB(self.newRowID, stt)

    def ETLProduct(self, df):
        """do the ETL job for product

        Args:
            df (dataframe): source df
        """
        sourceProduct = self._getSourceSystemProduct(df)
        dimProduct = spark.read.jdbc(url=createUrl(dbName=WAREHOUSE), table='dimProduct')

        if dimProduct.count() == 0:
            numOfNewProduct = sourceProduct.count()
            if numOfNewProduct != 0:
                sourceProduct.write.jdbc(url=createUrl(dbName=WAREHOUSE), table='dimProduct', mode='append')
                stt = f'Write {numOfNewProduct} products into db'
                print(stt)
                self._logToMetaDB(self.newRowID, stt)
            else:
                print('nothing new in source product')
        else:
            newProduct, updateProduct = self._getNewData(
                sourceProduct, 
                dimProduct, 
                ['ID Manufacturing Date', 'ID Expiry Date'], 
                ['Product ID', 'Product Name', 'Category', 'Sub-Category'])
            if updateProduct is not None:
                self._handleUpdatedDataProduct(updateProduct)
            numOfNewProduct = newProduct.count()
            if numOfNewProduct != 0:
                newProduct = newProduct.drop('idProduct')
                newProduct.write.jdbc(url=createUrl(dbName=WAREHOUSE), table='dimProduct', mode='append')
                stt = f'Write {numOfNewProduct} products into db'
                print(stt)
                self._logToMetaDB(self.newRowID, stt)
            else:
                print('nothing new in source product')

    def _getSourceSystemSale(self, df):
        """get the sale data from source

        Args:
            df (dataframe): source df

        Returns:
            dâtframe: source data (no duplication)
        """
        sourceSale = df[['Row ID', 'Order ID', 'Order Date', 'Ship Date', 'Ship Mode', 'Sales', 'Quantity', 'Discount', 'Profit', 
                        'Product ID', 'Product Name', 'Category', 'Sub-Category', 
                        'Customer ID', 'Customer Name', 'Segment', 'Country', 'City', 'State', 'Postal Code', 'Region']]
        sourceSale = sourceSale.withColumn('ID Order Date', sparkFunction.date_format(sourceSale['Order Date'], 'yyyyMMdd'))
        sourceSale = sourceSale.withColumn('ID Ship Date', sparkFunction.date_format(sourceSale['Ship Date'], 'yyyyMMdd'))
        sourceSale = sourceSale.drop('Ship Date', 'Order Date')

        dimProduct = spark.read.jdbc(url=createUrl(dbName=WAREHOUSE), table='dimProduct')
        sourceSale = sourceSale.join(dimProduct, on=['Product ID', 'Product Name', 'Category', 'Sub-Category'], how='inner')
        sourceSale = sourceSale.drop('Product ID', 'Product Name', 'Category', 'Sub-Category', 'ID Manufacturing Date', 'ID Expiry Date')
        
        dimCustomer = spark.read.jdbc(url=createUrl(dbName=WAREHOUSE), table='dimCustomer')
        sourceSale = sourceSale.join(dimCustomer, on=['Customer ID', 'Customer Name', 'Segment', 'Country', 'City', 'State', 'Postal Code', 'Region'], how='inner')
        sourceSale = sourceSale.drop('Customer ID', 'Customer Name', 'Segment', 'Country', 'City', 'State', 'Postal Code', 'Region', 'ID Manufacturing Date', 'ID Expiry Date')
        return sourceSale

    def ETLSaleDetail(self, df):
        """do the ETL job for sale detail table

        Args:
            df (dataframe): source df
        """
        sourceSale = self._getSourceSystemSale(df)

        numOfSource = sourceSale.count()
        if numOfSource != 0:
            sourceSale.write.jdbc(url=createUrl(dbName=WAREHOUSE), table='factSaleDetail', mode='append')
            stt = f'Write {numOfSource} records'
            print(stt)
            self._logToMetaDB(self.newRowID, stt)
        else:
            print('nothing new in source sale detail')

    def doETL(self):
        """do the whole ETL process
        """
        df = self._getSource()
        self.newRowID = self.oldRowID + df.count()
        self.ETLDate(df)
        self.ETLCustomer(df)
        self.ETLProduct(df)
        self.ETLSaleDetail(df)
        self._logToMetaDB(self.newRowID, 'finish')

etler = ETL()
etler.doETL()

cur.close()
conn.close()

cur1.close()
conn1.close()