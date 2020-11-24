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
