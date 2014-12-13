package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type Database struct {
	db *sql.DB
}

// NewDatabase makes connections to database and ping request to it
func NewDatabase(dsn string) (*Database, error) {
	Info.Print("Try to connect database by DSN: " + dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}
	Info.Print("Connection estabilished")
	return &Database{db: db}, nil
}

// QueryAndDump data, fetch them from data and send to storage
func (d *Database) QueryAndDump(tableName string, storage Storager, results chan<- Result) {
	Info.Print("Query from table: " + tableName)
	rows, err := d.db.Query("SELECT * FROM " + tableName)
	if err != nil {
		results <- Result{TableName: tableName, Error: err}
		return
	}

	columns, err := rows.Columns()
	if err != nil {
		results <- Result{TableName: tableName, Error: err}
		return
	}

	rowsChan := make(chan []string, 10)
	storage.Open(tableName, columns)
	storage.Put(rowsChan, results)

	ptrSlice := make([]interface{}, len(columns))
	for rows.Next() {
		valSlice := make([]string, len(columns))
		for key := range ptrSlice {
			ptrSlice[key] = &valSlice[key]
		}
		err := rows.Scan(ptrSlice...)
		if err != nil {
			results <- Result{TableName: tableName, Error: err}
			return
		}
		rowsChan <- valSlice
	}
	close(rowsChan)
}

// Close connection to database
func (d *Database) Close() {
	d.db.Close()
}
