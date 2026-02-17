# potatorf
Database management

# Building
To build `potatorf.c`, you only need [gcc](https://gcc.gnu.org/install/). 

Build command:
`gcc -O2 -o potatorf potatorf.c`

# How to use
- Command to load/make a database 
`./potatorf db.dbm` 
- Commands:
`CREATE TABLE`
`INSERT INFO`
`SELECT`
`UPDATE`
`DELETE FROM`
`DROP TABLE`
`SHOW TABLES`
`DESCRIBE`
`VACUUM`
`WHERE` (clauses with =, !=, <, >, <=, >=, IS NULL, IS NOT NULL)

- Colum types:
`INT`
`FLOAT`
`TEXT`
`BOOL`

# Examples

```
CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT, active BOOL);
INSERT INTO users VALUES (1, 'Alice', 30, true);
INSERT INTO users (id, name) VALUES (2, 'Bob');  -- age and active will be NULL
SELECT name, age FROM users WHERE age > 25;
UPDATE users SET active=false WHERE name='Alice';
DELETE FROM users WHERE age IS NULL;
SHOW TABLES;
DESCRIBE users;
VACUUM;
```
