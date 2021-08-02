use master 
go

drop database if exists SourceSystem
go 

create database SourceSystem
go 

use SourceSystem
go 

create table customer_order (
    [Row ID] INT,
    [Sales] NVARCHAR(50),
    [Quantity] NVARCHAR(20),
    [Discount] NVARCHAR(20),
    [Profit] FLOAT,
    [Order ID] NVARCHAR(20),
    [Order Date] DATETIME,
    [Ship Date] DATETIME,
    [Ship Mode] NVARCHAR(20),
    [Customer ID] NVARCHAR(20),
    [Customer Name] NVARCHAR(50),
    [Segment] NVARCHAR(20), -- kind of customer
    [Country] NVARCHAR(20),
    [City] NVARCHAR(20),
    [State] NVARCHAR(20),
    [Postal Code] INT,
    [Region] NVARCHAR(20),
    [Product ID] NVARCHAR(20),
    [Product Name] NVARCHAR(500),
    [Category] NVARCHAR(30),
    [Sub-Category] NVARCHAR(30),

    CONSTRAINT pk PRIMARY KEY([Row ID])
)
go

-- check datatype of table
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME='customer_order';

use SourceSystem
go
select * from customer_order