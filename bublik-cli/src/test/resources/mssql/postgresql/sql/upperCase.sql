create database test;
use test;
create schema test;

CREATE TABLE test.EmailAddress (
    BusinessEntityID int,
    EmailAddressID int,
    EmailAddress nvarchar(50),
    rowguid uniqueidentifier,
    ModifiedDate datetime,
    CONSTRAINT PK_EmailAddress_BusinessEntityID_EmailAddressID PRIMARY KEY (BusinessEntityID, EmailAddressID)
);
INSERT INTO test.EmailAddress (BusinessEntityID, EmailAddressID, EmailAddress, rowguid, ModifiedDate)
VALUES (1, 101, 'example@domain.com', NEWID(), GETDATE());
