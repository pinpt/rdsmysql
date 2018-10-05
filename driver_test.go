package rdsmysql

import (
	"database/sql"
	"fmt"
	"net/url"
	"sync"
	"testing"
)

type testLogger struct {
}

func (l *testLogger) Log(keyvals ...interface{}) error {
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

func TestDriver(t *testing.T) {
	db, err := sql.Open(DriverName, "root:@localhost/mysql")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	res, err := db.Query("select now()")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	for res.Next() {
		var val sql.NullString
		if err := res.Scan(&val); err != nil {
			t.Fatal(err)
		}
		if !val.Valid {
			t.Fatal("should have returned a value")
		}
	}
}

func TestDriverMultiple(t *testing.T) {
	count := 100
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db, err := sql.Open(DriverName, "root:@localhost/mysql")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			res, err := db.Query("select now()")
			if err != nil {
				t.Fatal(err)
			}
			defer res.Close()
			for res.Next() {
				var val sql.NullString
				if err := res.Scan(&val); err != nil {
					t.Fatal(err)
				}
				if !val.Valid {
					t.Fatal("should have returned a value")
				}
			}
		}()
	}
	wg.Wait()
}

func TestDriverPasswordEscape(t *testing.T) {
	up := url.UserPassword("foo", "xZ{G{V?X-R:y%l")
	db, err := sql.Open(DriverName, up.String()+"@localhost/mysql")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Query("select now()")
	if err == nil {
		t.Fatal("should have been an error")
	}
	assertEq(t, "Error 1045: Access denied for user 'foo'@'localhost' (using password: YES)", err.Error())
}

func TestGetReplicaHostname(t *testing.T) {
	c := &connection{
		hostname: "pinpt-78718787918791-rds-cluster.cluster-cyi98989111.us-east-1.rds.amazonaws.com",
	}
	replica, err := c.getReplicaHostname("pinpt-78718787918791-rds-node-1")
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, "pinpt-78718787918791-rds-node-1.cyi98989111.us-east-1.rds.amazonaws.com", replica)
}

func init() {
	L = &testLogger{}
}
