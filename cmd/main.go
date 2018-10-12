package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pinpt/rdsmysql"
)

type logger struct {
	mu sync.Mutex
}

func (l *logger) Log(keyvals ...interface{}) error {
	l.mu.Lock()
	kv := make(map[interface{}]interface{})
	kl := len(keyvals)
	for i := 0; i < kl; i += 2 {
		k := keyvals[i]
		var v interface{}
		if i+1 < kl {
			v = keyvals[i+1]
			kv[k] = v
		} else if k != nil {
			kv[k] = ""
		}
	}
	fmt.Print("[DEBUG] ")
	args := make([]interface{}, 0)
	if msg, ok := kv["msg"].(string); ok {
		args = append(args, msg)
		delete(kv, "msg")
		for k, v := range kv {
			args = append(args, fmt.Sprintf("%v=%v", k, v))
		}
		fmt.Println(args...)
	} else {
		if k, ok := keyvals[0].(string); ok {
			fmt.Printf(k, keyvals[1:]...)
			fmt.Println()
		}
	}
	l.mu.Unlock()
	return nil
}

var (
	debug          bool
	exec           bool
	load           bool
	concurrency    int
	recordCount    int
	iterationCount int
)

func init() {
	flag.BoolVar(&debug, "debug", false, "turn on debug logging")
	flag.BoolVar(&exec, "exec", false, "use exec instead of query")
	flag.BoolVar(&load, "load", false, "run a load test")
	flag.IntVar(&concurrency, "concurrency", 50, "when running load, use this concurrency")
	flag.IntVar(&recordCount, "records", 5000, "number of test records in the test table")
	flag.IntVar(&iterationCount, "iterations", 500, "number of query iterations to run")
}

var latinAndNumbers = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")

func random(count int, chars []byte) string {
	ra := make([]byte, count)
	_, err := rand.Read(ra)
	if err != nil {
		panic(err)
	}
	res := make([]byte, count)
	lenc := byte(len(chars))
	for i, b := range ra {
		res[i] = chars[b%lenc]
	}
	return string(res)
}

func loadQuery(ctx context.Context, db *sql.DB) {
	q := `
	SELECT t1.f1 FROM table1 AS t1
	INNER JOIN table1 AS t2 ON t2.parent = t1.id
	INNER JOIN table1 AS t3 ON t3.parent = t2.id
	INNER JOIN table1 AS t4 ON t4.parent = t3.id
	INNER JOIN table1 AS t5 ON t5.parent = t4.id
	INNER JOIN table1 AS t6 ON t6.parent = t5.id
	INNER JOIN table1 AS t7 ON t7.parent = t6.id
	WHERE t1.id > ?
	ORDER BY t1.id DESC LIMIT 100
	`
	rows, err := db.QueryContext(ctx, q, random(16, latinAndNumbers))
	if err != nil {
		select {
		case <-ctx.Done():
			return
		default:
		}
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var f1 string
		err := rows.Scan(&f1)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			panic(err)
		}
	}
}

func setupSchema(db *sql.DB) {
	if _, err := db.Exec(`
		DROP TABLE IF EXISTS table1;
		CREATE TABLE table1(id varchar(16) primary key, parent varchar(16), f1 text);
	`); err != nil {
		panic(err)
	}
}

func dropSchema(db *sql.DB) {
	db.Exec("DROP TABLE IF EXISTS table1")
}

func toString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func insertDataBatch(db *sql.DB) int {
	started := time.Now()
	fmt.Println("inserting test data...")
	var prev *string
	var buf strings.Builder
	for i := 0; i < recordCount; i++ {
		id := random(16, latinAndNumbers)
		text := random(1000, latinAndNumbers)
		buf.WriteString(fmt.Sprintf("INSERT INTO table1 (id, parent, f1) VALUES ('%s','%s','%s');", id, toString(prev), text))
		if i%1000 == 0 {
			_, err := db.Exec(buf.String())
			if err != nil {
				panic(err)
			}
			buf.Reset()
		}
		prev = &id
	}
	if buf.Len() > 0 {
		_, err := db.Exec(buf.String())
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("finished inserting test data, took %v\n", time.Since(started))
	return recordCount
}

func main() {
	flag.Parse()
	args := flag.Args()
	if load && len(args) != 1 {
		fmt.Printf("usage: %s <url>\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	} else if !load && len(args) != 2 {
		fmt.Printf("usage: %s <url> <query>\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}
	if debug {
		rdsmysql.L = &logger{}
	}
	if strings.Contains(args[0], "?") {
		args[0] = args[0] + "&multiStatements=true"
	} else {
		args[0] = args[0] + "?multiStatements=true"
	}
	db, err := sql.Open(rdsmysql.DriverName, args[0])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			cancel()
			db.Close()
			break
		}
	}()
	if load {
		setupSchema(db)
		insertDataBatch(db)
		defer dropSchema(db)
		var wg sync.WaitGroup
		started := time.Now()
		go func() {
			dumpStats := func() {
				stats := db.Stats()
				fmt.Printf("[STATS] idle=%d,inuse=%d,open=%d\n", stats.Idle, stats.InUse, stats.OpenConnections)
			}
		done:
			for {
				select {
				case <-time.After(10 * time.Second):
					dumpStats()
				case <-ctx.Done():
					break done
				}
			}
			dumpStats()
		}()
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for x := 0; x < iterationCount; x++ {
					loadQuery(ctx, db)
					msg := []interface{}{"msg", fmt.Sprintf("finished t=%03d, %04d/%04d", i, 1+x, iterationCount)}
					rdsmysql.L.Log(msg...)
				}
			}(i)
		}
		wg.Wait()
		fmt.Printf("finished %d iterations in %v\n", concurrency*iterationCount, time.Since(started))
	} else {
		if exec {
			r, err := db.ExecContext(ctx, args[1])
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			rows, _ := r.RowsAffected()
			fmt.Printf("%d rows affected\n", rows)
		} else {
			rows, err := db.QueryContext(ctx, args[1])
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
	}
}
