// This example aims to do some basic performance comparison between calculating statistics in Go and in DuckDB.
// It groups insertions into DuckDB into a transaction, but does not use the bulk insert API since this is not very well documented
// and it was difficult to find examples.
package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"slices"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

func CreateDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func CreateRecordsTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE SEQUENCE seq_records_id START 1;
		CREATE TABLE records (id INTEGER DEFAULT nextval('seq_records_id'), value DOUBLE)
	`)
	if err != nil {
		return err
	}
	return nil
}

func StatisticsFromRecords(records []Record) (float64, float64, float64, float64, float64) {
	var mean, median, stddev, min, max float64

	values := make([]float64, len(records))
	sum := 0.0
	max = math.Inf(-1)
	min = math.Inf(1)

	for i, r := range records {
		if r.Value > max {
			max = r.Value
		}
		if r.Value < min {
			min = r.Value
		}
		sum += r.Value
		values[i] = r.Value
	}
	slices.Sort(values)
	mean = sum / float64(len(records))

	for _, r := range records {
		stddev += (r.Value - mean) * (r.Value - mean)
	}
	stddev = math.Sqrt(stddev / float64(len(records)))

	if len(values)%2 == 0 {
		median = (values[len(values)/2-1] + values[len(values)/2]) / 2
	} else {
		median = values[len(values)/2]
	}

	return mean, median, stddev, min, max
}

func StatisticsFromDB(db *sql.DB) (float64, float64, float64, float64, float64) {
	rows, err := db.Query(`SELECT AVG(value), MEDIAN(value), STDDEV_POP(value), MIN(value), MAX(value) FROM records`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		log.Fatal("No rows returned")
	}
	var mean, median, stddev, min, max float64
	err = rows.Scan(&mean, &median, &stddev, &max, &min)
	if err != nil {
		log.Fatal(err)
	}
	return mean, median, stddev, min, max
}

func StandardInsert(records []Record, db *sql.DB) error {
	_, err := db.Exec(`BEGIN TRANSACTION`)
	if err != nil {
		return err
	}
	for _, record := range records {
		_, err = db.Exec("INSERT INTO records (value) VALUES (?)", record.Value)
		if err != nil {
			return err
		}
	}
	_, err = db.Exec(`COMMIT`)
	if err != nil {
		return err
	}
	return nil
}

type Record struct {
	ID    int
	Value float64
}

func main() {
	N := 1000000
	fmt.Printf("Inserting %d records into duckdb\n", N)
	db, err := CreateDB()
	if err != nil {
		log.Fatal("Error creating DuckDB database", err)
	}

	err = CreateRecordsTable(db)
	if err != nil {
		log.Fatal("Error creating records table", err)
	}

	// generate some data
	records := make([]Record, N)
	for i := 0; i < N; i++ {
		records[i] = Record{
			ID:    i,
			Value: float64(i),
		}
	}

	// time the insertion into DuckDB
	start := time.Now()
	err = StandardInsert(records, db)
	if err != nil {
		log.Fatal("Error inserting records", err)
	}
	insertionElapsed := time.Since(start)
	fmt.Printf("Insertion into DuckDB took: %s", insertionElapsed)

	// time the calculation of the statistics when calculating directly from Go structs
	start = time.Now()
	mean, median, stddev, min, max := StatisticsFromRecords(records)
	calculationElapsed := time.Since(start)
	fmt.Printf("Calculation of the  statistics\n\tmean: %f\n\tmedian: %f\n\tstddev: %f\n\tmin: %f\n\tmax: %f\n from records took: %s\n", mean, median, stddev, min, max, calculationElapsed)

	// time the calculation of the statistics when calculating using DuckDB
	start = time.Now()
	mean, median, stddev, min, max = StatisticsFromDB(db)
	calculationElapsed = time.Since(start)
	fmt.Printf("Calculation of the  statistics\n\tmean: %f\n\tmedian: %f\n\tstddev: %f\n\tmin: %f\n\tmax: %f\n from DB took: %s, total including insertion: %s\n", mean, median, stddev, min, max, calculationElapsed, calculationElapsed+insertionElapsed)
}
