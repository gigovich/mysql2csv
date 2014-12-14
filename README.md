mysql2csv
=========

Demo tool to dump MySQL tables into archived CSV files

ATTENTION!
----------
Don't use this tool in real systems. This is demo app wich makes dump
throug simple SELECT query.


Install
-------

Just run:

    go get github.com/gigovich/mysql2csv

Usage
-----

mysql2csv dumps all tables you defined in command line to archive folder.
Dump is gziped CSV file with content from this tables.

To print help, just run mysql2csv without arguments o with '--help' flag.

Simple usage:

    mysql2csv @/testdb table1 table2

First argument is DSN in PEAR DB format without type prefix.

DSN definition schema:

    [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]

All other arguments are table names to dump
