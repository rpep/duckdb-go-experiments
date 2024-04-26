package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	db, _ := sql.Open("duckdb", "")
	_, err := db.Exec("CREATE TABLE t (i INTEGER)")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for i := range 10 {
		_, err = db.Exec("INSERT INTO t VALUES (%s)", i)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	rows, _ := db.Query("SELECT * FROM t")
	for rows.Next() {
		var i int
		rows.Scan(&i)
		fmt.Println(i)
	}
}
