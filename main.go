package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// Result is a signal struct which used to
// monitor dumping proccess start, state and end.
type Result struct {
	TableName       string
	LinesProccessed int
	Finish          bool
	Error           error
}

var (
	Error *log.Logger
	Info  *log.Logger
)

func main() {
	var (
		helpFlag   bool
		silentFlag bool
	)

	flag.BoolVar(&helpFlag, "help", false, "print this help")
	flag.BoolVar(&silentFlag, "silent", false, "don't print info messages")
	flag.Parse()

	if helpFlag || flag.NArg() < 2 {
		fmt.Println()
		fmt.Println("Usage: mysq2csv [options] <DSN> <table_name> [table_name ...]")
		fmt.Println()
		fmt.Println("  <DSN> format: username:password@protocol+hostspec/database?option=value")
		fmt.Println()
		fmt.Println("Options:")
		flag.PrintDefaults()
		os.Exit(0)
	}

	// Disable logging info messages if silent enabled
	if silentFlag {
		Info = log.New(ioutil.Discard, "[INFO]: ", log.Ltime)
	} else {
		Info = log.New(os.Stdout, "[INFO]: ", log.Ltime|log.Lmicroseconds)
	}

	// Anyway error we must output to stderr
	Error = log.New(os.Stderr, "[ERROR]: ", log.LstdFlags)

	// Zero index argument is DSN string
	db, err := NewDatabase(flag.Arg(0))
	defer db.Close()
	if err != nil {
		log.Panic(err)
	}

	results := make(chan Result)
	tablesCount := 0
	for _, table := range flag.Args()[1:] {
		storage, err := NewCSVStorage(table)
		if err != nil {
			Error.Panic("Can't create archive file for table "+table+": ", err)
		}
		go db.QueryAndDump(table, storage, results)
		tablesCount++
	}

	for result := range results {
		if result.Finish {
			Info.Print(fmt.Sprintf("Processing '%v' table is finished, processed %v rows",
				result.TableName, result.LinesProccessed))
			tablesCount--
		} else if result.Error != nil {
			Error.Print(fmt.Sprintf("Error during process table %v: %v",
				result.TableName, result.Error))
			tablesCount--
		} else {
			Info.Print(fmt.Sprintf("In table '%v' processed %v rows",
				result.TableName, result.LinesProccessed))
		}
		if tablesCount == 0 {
			break
		}
	}
}
