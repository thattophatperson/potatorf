# potatorf 1.0
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

To create a table, here's an example:
`CREATE TABLE test (cool INT, cool2 INT);`
