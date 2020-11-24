# Setup

## PostgreSQL

  - Create a test database
  - Create a test table with a constraint and a trigger. You can use the script below.
 
```sql
CREATE TABLE person (
id uuid PRIMARY KEY,
number integer NOT NULL GENERATED ALWAYS AS IDENTITY (MINVALUE 0 START WITH 0 INCREMENT BY 1),
firstname text NULL,
lastname text NULL,
city text NULL DEFAULT 'Default City',
street text NOT NULL DEFAULT 'Default Street',
settingsid integer NOT NULL DEFAULT 0,
age integer NOT NULL
CONSTRAINT chk_person CHECK (age > 0 AND age < 101)
);

CREATE TABLE personhistory (
    number integer NOT NULL GENERATED ALWAYS AS IDENTITY (MINVALUE 0 START WITH 0 INCREMENT BY 1),
  name text
);

CREATE FUNCTION inserthistoryrow() RETURNS trigger AS $emp_stamp$
    BEGIN
        INSERT INTO public.personhistory(name) VALUES (CONCAT(NEW.firstname, ' ', NEW.lastname));
    
        RETURN NEW;
    END;
$emp_stamp$ LANGUAGE plpgsql;

CREATE TRIGGER personhistorytrigger AFTER INSERT ON person
    FOR EACH ROW EXECUTE PROCEDURE inserthistoryrow();
```

## MSSQL
  - Create a test database
  - Create a test table with a constraint and a trigger. You can use the script below.
  
   ```sql
CREATE TABLE [dbo].[Person] (
Id uniqueidentifier PRIMARY KEY,
Number int NOT NULL IDENTITY(1,1),
Firstname varchar(50) NULL,
Lastname varchar(50) NULL,
City varchar(50) NULL DEFAULT 'Default City',
Street varchar(50) NOT NULL DEFAULT 'Default Street',
SettingsId int NOT NULL DEFAULT 0,
Age int NOT NULL,
CONSTRAINT CHK_Person CHECK (Age > 0 AND Age <= 100)
);

CREATE TABLE [dbo].[PersonHistory] (
Number int NOT NULL IDENTITY(1,1),
Name varchar(100) NULL,
);

CREATE TRIGGER [dbo].[PersonHistoryTrigger]
   ON  [dbo].[Person] 
   AFTER INSERT
AS 
BEGIN
  -- SET NOCOUNT ON added to prevent extra result sets from
  -- interfering with SELECT statements.
  SET NOCOUNT ON;
  
  INSERT INTO dbo.PersonHistory (Name)
  SELECT CONCAT(Firstname, ' ', Lastname)
  FROM INSERTED

END
```

## SqlBulkCopy vs. PostgreSQL COPY

|                                                               | KeepIdentity                                                                                                                                                                                                                                                                   | CheckConstraint           | TableLock                                                                                                                                                 | KeepNulls                                                                                                                                             | FireTriggers                          | UseInternalTransaction                                                                                                                                                                          |
|---------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| new SqlBulkCopy(connectionString)                             |     Identity values are assigned by the destination (by the SQL Server) by default even if you set the value for such column manually.                                                                                                                                         | Constraints are not used. | I suppose it has the same behavior as SqlBulkCopyOptions.Default.                                                                                         | NULL values are replaced by default values where applicable. The DEFAULT value will be used even if the column was specified and the value is NULL.   | Triggers do not get fired.            | I suppose it has the same behavior as SqlBulkCopyOptions.Default.                                                                                                                               |
| new SqlBulkCopy(connectionString, SqlBulkCopyOptions.Default) | Identity values are assigned by the destination (by the SQL Server) by default even if you set the value for such column manually.                                                                                                                                             | Constraints are not used. | Row locks are used by default.                                                                                                                            | NULL values are replaced by default values where applicable. The DEFAULT value will be used even if the column was specified and the value is NULL.   | Triggers do not get fired by default. | By default all the data is a single batch. If one of the rows is failed then the whole transaction will be rolled back.                                                                         |
| npgsqlBinaryImporter.WriteRow(System.Data.DataRow row)        | Identity value which is assigned by a server will be used if the column was not specified, but if the column is specified then the user assigned valued will be used. E.g. if the column was specified, but the int value wasn’t set then the 0 will be written to a database. | Constraints are used.     | COPY FROM command does not do an exclusive access lock on the table it is writing to. It gets the same kind of lock as INSERT does, viz RowExclusiveLock. | Inserts NULL values instead of DEFAULT, if column was specified but a value wasn’t set. The DEFAULT value will be used if the column isn’t specified. | Triggers get fired by default.        | COPY FROM creates a single transaction if not run in a transaction; otherwise it will use the current transaction. If one of the rows is failed then the whole transaction will be rolled back. |