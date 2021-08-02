use master 
go 

drop database if exists MetaDatabase
go 

create database MetaDatabase
go 

use MetaDatabase
go 

create table History (
    id BIGINT IDENTITY(1,1),
    dateAdded DATE DEFAULT CURRENT_TIMESTAMP,
    timeAdded TIME DEFAULT CURRENT_TIMESTAMP,
    rowID INT, 
    detail NVARCHAR(1000),

    CONSTRAINT pk_History PRIMARY KEY(id)
)
go

use MetaDatabase
go
select * from History
