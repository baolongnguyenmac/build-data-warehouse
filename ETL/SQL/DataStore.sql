use master
go

drop database if exists DataStore
go

create database DataStore
go 

use DataStore 
go 

create table dimDate (
    [idDate] INT,
    [date] DATETIME,

    CONSTRAINT pk_date PRIMARY KEY([idDate])
)
go 

create table dimCustomer (
    [idCustomer] INT IDENTITY(1,1),
    [Customer ID] NVARCHAR(20),
    [Customer Name] NVARCHAR(50),
    [Segment] NVARCHAR(20),
    [Country] NVARCHAR(20),
    [City] NVARCHAR(20),
    [State] NVARCHAR(20),
    [Postal Code] INT,
    [Region] NVARCHAR(20),
    [ID Manufacturing Date] INT,
    [ID Expiry Date] INT,

    CONSTRAINT pk_customer PRIMARY KEY([idCustomer]),
    CONSTRAINT fk_customer_date FOREIGN KEY([ID Manufacturing Date]) REFERENCES dimDate([idDate]),
    CONSTRAINT fk_customer_date1 FOREIGN KEY([ID Expiry Date]) REFERENCES dimDate([idDate])
)
go 

-- create table dimOrder (
--     [Order ID] NVARCHAR(20),
--     [ID Order Date] INT,
--     [ID Ship Date] INT,
--     [Ship Mode] NVARCHAR(20),

--     CONSTRAINT pk_order PRIMARY KEY([Order ID]),
--     CONSTRAINT fk_order_date FOREIGN KEY([ID Order Date]) REFERENCES dimDate([idDate]),
--     CONSTRAINT fk_ship_date FOREIGN KEY([ID Ship Date]) REFERENCES dimDate([idDate])
-- )
-- go 

create table dimProduct (
    [idProduct] INT IDENTITY(1,1),
    [Product ID] NVARCHAR(20),
    [Product Name] NVARCHAR(500),
    [Category] NVARCHAR(30),
    [Sub-Category] NVARCHAR(30),
    [ID Manufacturing Date] INT,
    [ID Expiry Date] INT,

    CONSTRAINT pk_product PRIMARY KEY([idProduct])
)
go 

create table factSaleDetail (
    [Row ID] INT,

    [Order ID] NVARCHAR(20),
    [ID Order Date] INT,
    [ID Ship Date] INT,
    [Ship Mode] NVARCHAR(20),

    [Sales] NVARCHAR(50),
    [Quantity] NVARCHAR(20),
    [Discount] NVARCHAR(20),
    [Profit] FLOAT,

    [idCustomer] INT, -- extract idCUstomer bằng cách so khớp đống info về customer của source với dim
    [idProduct] INT, -- extract idProduct bằng cách so khớp đống info về product của source với dim

    CONSTRAINT pk_SaleDetail PRIMARY KEY([Row ID]),
    CONSTRAINT fk_SaleDetail_Customer FOREIGN KEY([idCustomer]) REFERENCES dimCustomer([idCustomer]),
    CONSTRAINT fk_SaleDetail_Product FOREIGN KEY([idProduct]) REFERENCES dimProduct([idProduct]),
    CONSTRAINT fk_order_date FOREIGN KEY([ID Order Date]) REFERENCES dimDate([idDate]),
    CONSTRAINT fk_ship_date FOREIGN KEY([ID Ship Date]) REFERENCES dimDate([idDate])
)
go 

use DataStore
go 

select * from dimCustomer
select * from dimProduct
-- select * from dimOrder
select * from dimDate
select * from factSaleDetail

-- check datatype of table
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME='dimDate';

