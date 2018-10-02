package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pinpt/rdsmysql"
)

type logger struct {
}

func (l *logger) Log(keyvals ...interface{}) error {
	kv := make(map[interface{}]interface{})
	for i := 0; i < len(keyvals); i += 2 {
		k := keyvals[i]
		v := keyvals[i+1]
		kv[k] = v
	}
	fmt.Print("[LOG] ")
	args := make([]interface{}, 0)
	if msg, ok := kv["msg"].(string); ok {
		args = append(args, msg)
		delete(kv, "msg")
		for k, v := range kv {
			args = append(args, fmt.Sprintf("%v=%v", k, v))
		}
		fmt.Println(args...)
	} else {
		fmt.Printf(keyvals[0].(string), keyvals[1:]...)
		fmt.Println()
	}
	return nil
}

var debug bool

func init() {
	flag.BoolVar(&debug, "debug", false, "turn on debug logging")
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) != 2 {
		fmt.Printf("usage: %s <url> <query>\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}
	if debug {
		rdsmysql.L = &logger{}
	}
	db, err := sql.Open(rdsmysql.DriverName, args[0])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer db.Close()
	rows, err := db.Query(args[1])
	if err != nil && err != sql.ErrNoRows {
		fmt.Println(err)
		os.Exit(1)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for _, col := range cols {
		fmt.Print(col, "\t")
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 120))
	for rows.Next() {
		vals := make([]interface{}, 0)
		for i := 0; i < len(cols); i++ {
			var s sql.NullString
			vals = append(vals, &s)
		}
		if err := rows.Scan(vals...); err != nil {
			fmt.Println("error scanning rows: ", err)
			os.Exit(1)
		}
		for _, val := range vals {
			s := val.(*sql.NullString)
			fmt.Print(s.String, "\t")
		}
		fmt.Println()
	}
}
