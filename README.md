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
  