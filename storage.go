package main

import (
	"compress/gzip"
	"encoding/csv"
	"io"
	"os"
	"path"
)

// Storager interface used by database fetching functions
// to put data in structs realizing them
type Storager interface {
	// Open will be called allways before Put. It's setups
	// runtime params, such as table and columns names
	Open(tableName string, columns []string)

	// Put data from rows channel to storage. This method
	// can be called as regular function, so real data fetching
	// and processing must be done in saparate gorutine
	Put(rows <-chan []string, results chan<- Result)
}

// CSVStorage realizes comma-separated values file storage
// with gziping mechanizm
type CSVStorage struct {
	tableName  string
	columns    []string
	csvWriter  *csv.Writer
	gzipWriter io.WriteCloser
	fileWriter io.WriteCloser
}

// NewCSVStorage creates CSVStorage struct with already opened file for data save
func NewCSVStorage(tableName string) (*CSVStorage, error) {
	err := os.Mkdir("archive", 0755)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}

	file, err := os.OpenFile(path.Join("archive", tableName+".gz"), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	storage := &CSVStorage{}
	storage.fileWriter = file
	storage.gzipWriter = gzip.NewWriter(file)
	storage.csvWriter = csv.NewWriter(storage.gzipWriter)
	return storage, nil
}

func (s *CSVStorage) Open(tableName string, columns []string) {
	s.tableName = tableName
	s.columns = columns
	// You can add "headers" to CSV file here
	// s.csvWriter(columns)
}

func (s *CSVStorage) Put(rows <-chan []string, results chan<- Result) {
	// hide internal implementation
	// Here we make some prepare actions, for example add timer
	go s.process(rows, results)
}

// process is internal gorutine whichs do real job to save data
func (s *CSVStorage) process(rows <-chan []string, results chan<- Result) {
	count := 0
	for row := range rows {
		err := s.csvWriter.Write(row)
		if err != nil {
			results <- Result{TableName: s.tableName, Error: err}
			return
		}
		count++
		if count%50000 == 0 {
			results <- Result{TableName: s.tableName, LinesProccessed: count}
		}
	}

	s.csvWriter.Flush()

	err := s.gzipWriter.Close()
	if err != nil {
		results <- Result{TableName: s.tableName, Error: err}
		return
	}

	err = s.fileWriter.Close()
	if err != nil {
		results <- Result{TableName: s.tableName, Error: err}
		return
	}

	results <- Result{TableName: s.tableName, LinesProccessed: count, Finish: true}
}
