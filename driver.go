package rdsmysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

// defaultUpdateTopology specifies how often the client should query the sql server for current servers list
const defaultUpdateTopology = 30 * time.Second

// defaultMaxTimeLeaving is the delay after node was removed from replica_host_status or query failed and db.Close is called. Also required for long running queries to allow interrupting iterating rows.
//
// Shorter time cleans up resources faster.
// Longer time reuses db instance and connection for temporary errors.
//
// Keep shorter than failDuration to be able to test it on node restarts, otherwise it will only be executed on node deletes (it would still work, but better to test more often to avoid bugs in code).
const defaultMaxTimeLeaving = 3 * 60 * time.Second

// defaultFailDuration is the duration the node is marked as failed after failing query. The node is ignored for this duration.
//
// Shorter time re-connects faster for restarts and temporary errors.
// Longer time avoids unnecessary re-tries for shutdowns.
//
// It takes about ~4 min to a host to be removed from information_schema.replica_host_status after shutdown and first failed query.
const defaultFailDuration = time.Minute

// defaultConnectTimeout is the duration the client will wait for connection to database before timing out
const defaultConnectTimeout = 5 * time.Second

// defaultReadimeout is the duration the client will wait for read buffer
const defaultReadimeout = 5 * time.Minute

// ErrNoServersAvailable is returned from Query when no servers are available. All servers returned from information_schema.replica_host_status failed.
var ErrNoServersAvailable = errors.New("no servers available")

// ErrConnectMaxRetriesExceeded is returned from Query when query tried more than MaxServersTriedForQuery servers, and all of them failed. There could be more available instances.
var ErrConnectMaxRetriesExceeded = errors.New("tried MaxServersTriedForQuery servers to connect, all failed")

// ErrExecMaxRetriesExceeded is returned after exhausting a retryable query
var ErrExecMaxRetriesExceeded = errors.New("max retries exceeded for retryable error")

// ErrUnsupportedMethod is returned for an unsupported method that is invoked against the underlying driver
var ErrUnsupportedMethod = errors.New("unsupported method")

// MaxServersTriedForQuery is the max number of servers tried for a query. The actual number of retries is +1 since it tries to connect to initial instance twice.
const MaxServersTriedForQuery = 3

// MaxConnectAttempts is the max number of tries to connect to a server
const MaxConnectAttempts = 3

// Logger is the fundamental interface for all log operations. Log creates a
// log event from keyvals, a variadic sequence of alternating keys and values.
// Implementations must be safe for concurrent use by multiple goroutines. In
// particular, any implementation of Logger that appends to keyvals or
// modifies or retains any of its elements must make a copy first.
type Logger interface {
	Log(keyvals ...interface{}) error
}

type defaultLogger struct {
}

func (l *defaultLogger) Log(keyvals ...interface{}) error {
	return nil
}

// L is the logger to use internally by the package, defaults to a logger which is no op
var L Logger = &defaultLogger{}

type db struct {
}

// make sure our db implements the full driver interface
var _ driver.Driver = (*db)(nil)

type connection struct {
	topology        *topology
	mu              sync.Mutex
	connections     map[string]driver.Conn
	hostname        string
	port            int
	database        string
	args            url.Values
	userinfo        *url.Userinfo
	updateDuration  time.Duration
	topologyUpdates *ticker
	noreplicas      bool
}

// make sure our connection implements the full driver.Conn interfaces
var _ driver.Conn = (*connection)(nil)
var _ driver.Queryer = (*connection)(nil)
var _ driver.QueryerContext = (*connection)(nil)
var _ driver.Execer = (*connection)(nil)
var _ driver.ExecerContext = (*connection)(nil)
var _ driver.Pinger = (*connection)(nil)
var _ driver.ConnBeginTx = (*connection)(nil)
var _ driver.SessionResetter = (*connection)(nil)
var _ driver.ConnPrepareContext = (*connection)(nil)

type statement struct {
	query  string
	conn   *connection
	parent driver.Stmt
	ctx    context.Context
}

// make sure our statement implements the full driver.Stmt interfaces
var _ driver.Stmt = (*statement)(nil)
var _ driver.StmtExecContext = (*statement)(nil)
var _ driver.StmtQueryContext = (*statement)(nil)

var mysqlDriver mysql.MySQLDriver

func parseDuration(key string, q url.Values, def time.Duration) (time.Duration, error) {
	val := q.Get(key)
	if val == "" {
		return def, nil
	}
	return time.ParseDuration(val)
}

// DefaultDriverOpts are the default driver values if not provided in the DSN
var DefaultDriverOpts = url.Values{
	"collation":   {"utf8mb4_unicode_ci"},
	"parseTime":   {"true"},
	"autocommit":  {"true"},
	"timeout":     {defaultConnectTimeout.String()},
	"readTimeout": {defaultReadimeout.String()},
}

// TrackedConn is an interface that the conn implementation returned from the Open
// returns to allow it to be terminated
type TrackedConn interface {
	// Terminate will forceably shutdown the connection and close all db
	Terminate()
}

var _ TrackedConn = (*connection)(nil)

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
//
// Open may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The returned connection is only used by one goroutine at a
// time.
func (d *db) Open(name string) (driver.Conn, error) {
	if !strings.Contains(name, "//") {
		// make sure it's parsed as a full url
		name = "//" + name
	}
	L.Log("msg", "open", "url", name)
	u, err := url.Parse(name)
	if err != nil {
		return nil, err
	}
	port := 3306
	if u.Port() != "" {
		port, err = strconv.Atoi(u.Port())
		if err != nil {
			return nil, fmt.Errorf("error parsing port: %v", err)
		}
	}
	dbname := u.EscapedPath()
	if dbname != "" {
		if dbname[0] == '/' {
			dbname = dbname[1:]
		}
	}
	q := u.Query()
	maxTimeLeaving, err := parseDuration("max-time-leaving", q, defaultMaxTimeLeaving)
	if err != nil {
		return nil, fmt.Errorf("error parsing max-time-duration: %v", err)
	}
	failDuration, err := parseDuration("fail-duration", q, defaultFailDuration)
	if err != nil {
		return nil, fmt.Errorf("error parsing fail-duration: %v", err)
	}
	updateDuration, err := parseDuration("update-duration", q, defaultUpdateTopology)
	if err != nil {
		return nil, fmt.Errorf("error parsing update-duration: %v", err)
	}

	// remove these since we don't want to pass them to mysql driver
	q.Del("max-time-leaving")
	q.Del("fail-duration")
	q.Del("update-duration")

	// make sure our default driver values are set if not provided
	for k, v := range DefaultDriverOpts {
		if q.Get(k) == "" {
			q.Set(k, v[0])
		}
	}

	conn := &connection{
		hostname:       u.Hostname(),
		port:           port,
		database:       dbname,
		args:           q,
		userinfo:       u.User,
		updateDuration: updateDuration,
		connections:    make(map[string]driver.Conn),
	}
	conn.topology = newTopology(topologyOpts{
		MaxTimeLeaving: maxTimeLeaving,
		OnFound: func(id string) {
			L.Log("msg", "database server found", "id", id)
		},
		OnFailed: func(id string) {
			L.Log("msg", "database server failed", "id", id)
		},
		OnLeave: func(id string) {
			L.Log("msg", "database server leaving", "id", id)
			conn.mu.Lock()
			defer conn.mu.Unlock()
			c, ok := conn.connections[id]
			if !ok {
				// already left
				return
			}
			go func(db driver.Conn) {
				db.Close()
			}(c)
			delete(conn.connections, id)
		},
		FailDuration: failDuration,
		Logger:       L,
		Now:          time.Now,
	})

	if err := conn.updateTopologyInitial(); err != nil {
		return nil, err
	}

	conn.setupTopologyTicker()
	return conn, nil
}

// Terminate will forceably shutdown the connection and close all db
func (c *connection) Terminate() {
	L.Log("msg", "terminate", "hostname", c.hostname)
	c.topologyUpdates.Stop()
	c.mu.Lock()
	for _, conn := range c.connections {
		conn.Close()
	}
	c.connections = nil
	c.mu.Unlock()
}

func (c *connection) updateTopology() error {
	L.Log("msg", "update topology called", "hostname", c.hostname)
	conn, err := c.getConnection(c.hostname, true)
	if err != nil {
		return err
	}
	defer conn.Close()
	res, err := c.retrieveTopology(conn)
	if err != nil {
		return err
	}
	c.topology.SetAvailableFromReplicaHostStatus(c.hostname, res)
	return nil
}

func (c *connection) setupTopologyTicker() {
	c.topologyUpdates = newTicker(c.updateDuration)
	go func() {
	LOOP:
		for {
			select {
			case <-c.topologyUpdates.ticker.C:
				c.updateTopology()
				c.topology.ExecuteOnLeaveIfNeeded()
			case <-c.topologyUpdates.stop:
				break LOOP
			}
		}
		c.topologyUpdates.stopped <- true
	}()
}

var profileReg map[string]*tls.Config
var profileMu sync.RWMutex

const rdsHostnamaeSuffix = "rds.amazonaws.com"

// GetDSN is a helper for getting the DSN to mysql
func GetDSN(hostname string, port int, username string, password string, name string, args url.Values) (string, error) {
	// args are modified below (setting tls)
	// to prevent concurrent modifications create a full copy
	args, err := url.ParseQuery(args.Encode())
	if err != nil {
		return "", nil
	}
	// if running in AWS RDS, make sure we register the special TLS certs
	if strings.Contains(hostname, rdsHostnamaeSuffix) {
		var tlsConfig *tls.Config
		profile := hex.EncodeToString([]byte(hostname))
		// first check to see if we already have a profile registered for this hostname
		profileMu.RLock()
		tlsConfig = profileReg[profile]
		profileMu.RUnlock()
		// if not, we need to register it
		if tlsConfig == nil {
			profileMu.Lock()
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM([]byte(mysqlRDSCACert)) {
				return "", fmt.Errorf("can't add RDS TLS certs")
			}
			tlsConfig = &tls.Config{
				ServerName: hostname,
				RootCAs:    caCertPool,
			}
			err := mysql.RegisterTLSConfig(profile, tlsConfig)
			if err != nil {
				return "", err
			}
			profileMu.Unlock()
		}
		args.Set("tls", profile)
	}
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", hostname, port)
	cfg.User = username
	cfg.Passwd = password
	cfg.DBName = name
	return cfg.FormatDSN() + "?" + args.Encode(), nil
}

func (c *connection) getConnection(hostname string, master bool) (driver.Conn, error) {
	pass, _ := c.userinfo.Password()
	dsn, err := GetDSN(hostname, c.port, c.userinfo.Username(), pass, c.database, c.args)
	if err != nil {
		return nil, err
	}
	if master {
		dsn += "&rejectReadOnly=true" // for master, we need to reject readonly. see https://github.com/go-sql-driver/mysql#rejectreadonly
	}
	L.Log("msg", "new connection", "hostname", hostname)
	for i := 0; i < MaxConnectAttempts+1; i++ {
		conn, err := mysqlDriver.Open(dsn)
		if isExecRetryable(err) {
			exponentialBackoff(i + 1)
			continue
		}
		return conn, err
	}
	L.Log("msg", "couldn't open connect after max attempts", "hostname", hostname)
	return nil, ErrConnectMaxRetriesExceeded
}

const maxReplicaLagMs = 100

const clusterPrefix = ".cluster-"

func (c *connection) getReplicaHostname(serverid string) (string, error) {
	// serverid will be something like foo-9999111
	// which needs to turn into something like foo-9999111-rds-node-0.aksjlfajsldfjasdf.us-east-1.rds.amazonaws.com
	// from the master which is something like foo-9999111-rds-cluster.cluster-aksjlfajsldfjasdf.us-east-1.rds.amazonaws.com
	hostname := c.hostname
	i := strings.Index(hostname, clusterPrefix)
	if i < 0 {
		return "", fmt.Errorf("couldn't determine the correct RDS replica hostname from initial master url")
	}
	return fmt.Sprintf("%s.%s", serverid, hostname[i+9:]), nil
}

func (c *connection) retrieveTopology(conn driver.Conn) ([]string, error) {
	// TODO: look into only running one of these to service all connections instead of N per connection
	L.Log("msg", "retrieve topology called")
	// find all the replicas
	q := fmt.Sprintf(`
		SELECT server_id
		FROM information_schema.replica_host_status
		WHERE replica_lag_in_milliseconds < %d AND
		session_id <> 'MASTER_SESSION_ID'
	`, maxReplicaLagMs)
	stmt, err := conn.Prepare(q)
	if err != nil {
		// this is in case where the MYSQL db we connect do isn't RDS in which case the replica_host_status
		// table won't exist (this error) and so we just silently act like there are no replicas
		if strings.Contains(err.Error(), "Error 1109") {
			return []string{}, nil
		}
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []string
	for {
		row := make([]driver.Value, 1)
		if err := rows.Next(row); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		buf := row[0].([]uint8)
		// convert our replica server id into the full RDS hostname
		hostname, err := c.getReplicaHostname(string(buf))
		if err != nil {
			return nil, err
		}
		L.Log("msg", "found replica", "hostname", hostname)
		res = append(res, hostname)
	}
	return res, nil
}

// updateTopologyInitial will fetch the initial topology
func (c *connection) updateTopologyInitial() error {
	for i := 0; i < MaxRetries+1; i++ {
		conn, err := c.getConnection(c.hostname, true)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := c.retrieveTopology(conn)
		if err != nil {
			return err
		}
		L.Log("msg", "updating topology initial", "servers", res, "hostname", c.hostname)
		c.topology.SetAvailableFromReplicaHostStatus(c.hostname, res)
		return nil
	}
	L.Log("msg", "update topology initial, couldn't get connection", "hostname", c.hostname)
	return ErrConnectMaxRetriesExceeded
}

func randn(maxNotInclusive int) int {
	return rand.Intn(maxNotInclusive)
}

// getMaster returns the master connection
func (c *connection) getMaster() (driver.Conn, error) {
	c.mu.Lock()
	db, ok := c.connections[c.hostname]
	c.mu.Unlock()
	if !ok {
		conn, err := c.getConnection(c.hostname, true)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.connections[c.hostname] = conn
		c.mu.Unlock()
		db = conn
	}
	return db, nil
}

func (c *connection) getReplicaCount() int {
	available := c.topology.GetAvailable()
	if available == nil {
		return 1
	}
	return len(available)
}

// getReplica returns an available replica out of random selection if more than 1
func (c *connection) getReplica() (driver.Conn, string, error) {
	available := c.topology.GetAvailable()
	empty := available == nil || len(available) == 0

	if empty {
		// attempt to use the master if no replicas are available
		if !c.noreplicas {
			// only show it once for a connection
			L.Log("msg", "no replicas found, will attempt to use the master")
			c.noreplicas = true
		}
		conn, err := c.getMaster()
		if err != nil {
			return nil, "", err
		}
		if conn != nil {
			return conn, c.hostname, nil
		}
		return nil, "", ErrNoServersAvailable
	}

	var hostname string
	for len(available) > 0 {
		if len(available) > 1 {
			hostname = available[randn(len(available))]
		} else {
			hostname = available[0]
		}
		c.mu.Lock()
		db, ok := c.connections[hostname]
		c.mu.Unlock()
		if !ok {
			conn, err := c.getConnection(hostname, false)
			if err != nil {
				return nil, hostname, err
			}
			c.mu.Lock()
			c.connections[hostname] = conn
			c.mu.Unlock()
			db = conn
		}
		return db, hostname, nil
	}

	// attempt to use the master if no replicas are available
	if !c.noreplicas {
		// only show it once for a connection
		L.Log("msg", "no replicas found, will attempt to use the master")
		c.noreplicas = true
	}
	conn, err := c.getMaster()
	if err != nil {
		return nil, c.hostname, err
	}
	if conn != nil {
		return conn, c.hostname, nil
	}
	return nil, "", ErrNoServersAvailable
}

// Prepare returns a prepared statement, bound to this connection.
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return &statement{query, c, nil, context.Background()}, nil
}

func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &statement{query, c, nil, ctx}, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *connection) Close() error {
	// don't shutdown the connection, we'll internally handle it
	c.Terminate()
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *connection) Begin() (driver.Tx, error) {
	conn, err := c.getMaster()
	if err != nil {
		return nil, err
	}
	return conn.Begin()
}

// BeginTx starts and returns a new transaction.
// If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
//
// This must check opts.Isolation to determine if there is a set
// isolation level. If the driver does not support a non-default
// level and one is set or if there is a non-default isolation level
// that is not supported, an error must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only
// value is true to either set the read-only transaction property if supported
// or return an error if it is not supported.
func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	conn, err := c.getMaster()
	if err != nil {
		return nil, err
	}
	if c, ok := conn.(driver.ConnBeginTx); ok {
		return c.BeginTx(ctx, opts)
	}
	return conn.Begin()
}

// ResetSession is called while a connection is in the connection
// pool. No queries will run on this connection until this method returns.
//
// If the connection is bad this should return driver.ErrBadConn to prevent
// the connection from being returned to the connection pool. Any other
// error will be discarded.
func (c *connection) ResetSession(ctx context.Context) error {
	return nil
}

// Pinger is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement Pinger, the sql package's DB.Ping and
// DB.PingContext will check if there is at least one Conn available.
//
// If Conn.Ping returns ErrBadConn, DB.Ping and DB.PingContext will remove
// the Conn from pool.
func (c *connection) Ping(ctx context.Context) error {
	conn, err := c.getMaster()
	if err != nil {
		return err
	}
	if p, ok := conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return ErrUnsupportedMethod
}

// Execer is an optional interface that may be implemented by a Conn.
//
// If a Conn implements neither ExecerContext nor Execer Execer,
// the sql package's DB.Exec will first prepare a query, execute the statement,
// and then close the statement.
//
// Exec may return ErrSkip.
//
// Deprecated: Drivers should implement ExecerContext instead.
func (c *connection) Exec(query string, args []driver.Value) (driver.Result, error) {
	for i := 0; i < MaxServersTriedForQuery+1; i++ {
		conn, err := c.getMaster()
		if err != nil {
			if err == ErrNoServersAvailable || isExecRetryable(err) {
				exponentialBackoff(i + 1)
				continue
			}
			return nil, err
		}
		if mc, ok := conn.(driver.Execer); ok {
			res, err := mc.Exec(query, args)
			if isExecRetryable(err) {
				exponentialBackoff(i + 1)
				continue
			}
			return res, err
		}
		return nil, ErrUnsupportedMethod
	}
	return nil, ErrExecMaxRetriesExceeded
}

// ExecerContext is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement ExecerContext, the sql package's DB.Exec
// will fall back to Execer; if the Conn does not implement Execer either,
// DB.Exec will first prepare a query, execute the statement, and then
// close the statement.
//
// ExecerContext may return ErrSkip.
//
// ExecerContext must honor the context timeout and return when the context is canceled.
func (c *connection) ExecContext(ctx context.Context, query string, nargs []driver.NamedValue) (driver.Result, error) {
	for i := 0; i < MaxServersTriedForQuery+1; i++ {
		conn, err := c.getMaster()
		if err != nil {
			if err == ErrNoServersAvailable || isExecRetryable(err) {
				exponentialBackoff(i + 1)
				continue
			}
			return nil, err
		}
		if mc, ok := conn.(driver.ExecerContext); ok {
			res, err := mc.ExecContext(ctx, query, nargs)
			if isExecRetryable(err) {
				exponentialBackoff(i + 1)
				continue
			}
			return res, err
		}
		return nil, ErrUnsupportedMethod
	}
	return nil, ErrExecMaxRetriesExceeded
}

// Queryer is an optional interface that may be implemented by a Conn.
//
// If a Conn implements neither QueryerContext nor Queryer,
// the sql package's DB.Query will first prepare a query, execute the statement,
// and then close the statement.
//
// Query may return ErrSkip.
//
// Deprecated: Drivers should implement QueryerContext instead.
func (c *connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	attempts := c.getReplicaCount()
	for i := 0; i < attempts+1; i++ {
		conn, hostname, err := c.getReplica()
		if err != nil {
			if err == ErrNoServersAvailable {
				c.topology.MarkFailed(hostname)
				continue
			}
			return nil, err
		}
		if s, ok := conn.(driver.Queryer); ok {
			rows, err := s.Query(query, args)
			if isQueryRetryable(err) {
				// on an error, mark it as closed
				if shouldMarkConnectionBad(err) {
					c.topology.MarkFailed(hostname)
				}
				exponentialBackoff(i + 1)
				continue
			}
			return rows, err
		}
		return nil, ErrUnsupportedMethod
	}
	L.Log("msg", "couldn't find replica for query")
	return nil, ErrConnectMaxRetriesExceeded
}

// QueryerContext is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement QueryerContext, the sql package's DB.Query
// will fall back to Queryer; if the Conn does not implement Queryer either,
// DB.Query will first prepare a query, execute the statement, and then
// close the statement.
//
// QueryerContext may return ErrSkip.
//
// QueryerContext must honor the context timeout and return when the context is canceled.
func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	attempts := c.getReplicaCount()
	for i := 0; i < attempts+1; i++ {
		conn, hostname, err := c.getReplica()
		if err != nil {
			if err == ErrNoServersAvailable {
				c.topology.MarkFailed(hostname)
				continue
			}
			return nil, err
		}
		if s, ok := conn.(driver.QueryerContext); ok {
			rows, err := s.QueryContext(ctx, query, args)
			if isQueryRetryable(err) {
				// on an error, mark it as closed
				if shouldMarkConnectionBad(err) {
					c.topology.MarkFailed(hostname)
				}
				exponentialBackoff(i + 1)
				continue
			}
			return rows, err
		}
		return nil, ErrUnsupportedMethod
	}
	L.Log("msg", "couldn't find replica for query")
	return nil, ErrConnectMaxRetriesExceeded
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (s *statement) Close() error {
	if s.parent != nil {
		return s.parent.Close()
	}
	return nil
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *statement) NumInput() int {
	if s.parent == nil {
		return -1
	}
	return s.parent.NumInput()
}

func (s *statement) checkForRetry(i int, hostname string, conn driver.Conn, st driver.Stmt, err error) bool {
	if isQueryRetryable(err) {
		if st != nil {
			st.Close()
		}
		if conn != nil {
			conn.Close()
		}
		s.parent = nil
		// on an error, mark it as closed
		if shouldMarkConnectionBad(err) {
			s.conn.topology.MarkFailed(hostname)
		}
		exponentialBackoff(i + 1)
		return true
	}
	return false
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *statement) Query(args []driver.Value) (driver.Rows, error) {
	attempts := s.conn.getReplicaCount()
	for i := 0; i < attempts+1; i++ {
		conn, hostname, err := s.conn.getReplica()
		if err != nil {
			if err == ErrNoServersAvailable {
				s.conn.topology.MarkFailed(hostname)
				continue
			}
			return nil, err
		}
		var st driver.Stmt
		if cp, ok := conn.(driver.ConnPrepareContext); ok {
			st, err = cp.PrepareContext(s.ctx, s.query)
		} else {
			st, err = conn.Prepare(s.query)
		}
		if s.checkForRetry(i, hostname, conn, st, err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		s.parent = st
		rows, err := st.Query(args)
		if s.checkForRetry(i, hostname, conn, st, err) {
			continue
		}
		return rows, err
	}
	L.Log("msg", "couldn't find replica for query")
	return nil, ErrConnectMaxRetriesExceeded
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (s *statement) QueryContext(ctx context.Context, nargs []driver.NamedValue) (driver.Rows, error) {
	args, err := namedValueToValue(nargs)
	if err != nil {
		return nil, err
	}
	attempts := s.conn.getReplicaCount()
	for i := 0; i < attempts; i++ {
		select {
		case <-ctx.Done():
			return nil, driver.ErrSkip
		default:
			break
		}
		conn, hostname, err := s.conn.getReplica()
		if err != nil {
			if err == ErrNoServersAvailable {
				s.conn.topology.MarkFailed(hostname)
				continue
			}
			return nil, err
		}
		var st driver.Stmt
		if cp, ok := conn.(driver.ConnPrepareContext); ok {
			st, err = cp.PrepareContext(s.ctx, s.query)
		} else {
			st, err = conn.Prepare(s.query)
		}
		if s.checkForRetry(i, hostname, conn, st, err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		s.parent = st
		rows, err := st.Query(args)
		if s.checkForRetry(i, hostname, conn, st, err) {
			continue
		}
		return rows, err
	}
	L.Log("msg", "couldn't find replica for query")
	return nil, ErrConnectMaxRetriesExceeded
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *statement) Exec(args []driver.Value) (driver.Result, error) {
	conn, err := s.conn.getMaster()
	if err != nil {
		return nil, err
	}
	var st driver.Stmt
	if cp, ok := conn.(driver.ConnPrepareContext); ok {
		st, err = cp.PrepareContext(s.ctx, s.query)
	} else {
		st, err = conn.Prepare(s.query)
	}
	if err != nil {
		return nil, err
	}
	s.parent = st
	for i := 0; i < MaxRetries; i++ {
		res, err := st.Exec(args)
		if isExecRetryable(err) {
			exponentialBackoff(i + 1)
			continue
		}
		return res, err
	}
	L.Log("msg", "couldn't find master for exec")
	return nil, ErrExecMaxRetriesExceeded
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (s *statement) ExecContext(ctx context.Context, nargs []driver.NamedValue) (driver.Result, error) {
	args, err := namedValueToValue(nargs)
	if err != nil {
		return nil, err
	}
	conn, err := s.conn.getMaster()
	if err != nil {
		return nil, err
	}
	var st driver.Stmt
	if cp, ok := conn.(driver.ConnPrepareContext); ok {
		st, err = cp.PrepareContext(s.ctx, s.query)
	} else {
		st, err = conn.Prepare(s.query)
	}
	if err != nil {
		return nil, err
	}
	s.parent = st
	var res driver.Result
	for i := 0; i < MaxRetries; i++ {
		if st2, ok := st.(driver.StmtExecContext); ok {
			res, err = st2.ExecContext(ctx, nargs)
		} else {
			res, err = st.Exec(args)
		}
		if isExecRetryable(err) {
			exponentialBackoff(i + 1)
			continue
		}
		return res, err
	}
	L.Log("msg", "couldn't find master for exec")
	return nil, ErrExecMaxRetriesExceeded
}

// namedValueToValue is a helper function copied from the database/sql package
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}

// MaxRetries is the max number of times that a deadlock query will be retried
var MaxRetries = 3

func isInvalidConnection(err error) bool {
	return err == mysql.ErrInvalidConn || err.Error() == "invalid connection" || strings.Contains(err.Error(), "connection refused") || isIOTimeoutError(err) || err == ErrConnectMaxRetriesExceeded
}

func isIOTimeoutError(err error) bool {
	if err, ok := err.(net.Error); ok && err.Timeout() {
		return true
	}
	return false
}

func shouldMarkConnectionBad(err error) bool {
	return isInvalidConnection(err) || err == sql.ErrConnDone || err == driver.ErrBadConn || err == io.EOF
}

func isExecRetryable(err error) bool {
	if err != nil {
		if err == driver.ErrSkip {
			return false
		}
		if isInvalidConnection(err) || err == sql.ErrConnDone || err == driver.ErrBadConn || err == io.EOF {
			return true
		}
		if strings.Contains(err.Error(), "Error 1213: Deadlock found when trying to get lock") {
			// this is a retryable query
			return true
		}
	}
	return false
}

func isQueryRetryable(err error) bool {
	if err != nil {
		if err == driver.ErrSkip {
			return false
		}
		// fmt.Println("[DEBUG] err detected in query", err, ">>>", err.Error(), ">>> type", reflect.TypeOf(err))
		if isInvalidConnection(err) || err == sql.ErrConnDone || err == driver.ErrBadConn || err == io.EOF {
			return true
		}
	}
	return false
}

func exponentialBackoffValue(retryCount int) time.Duration {
	return time.Millisecond * 200 * time.Duration(math.Pow(2, float64(retryCount)))
}

func exponentialBackoff(retryCount int) {
	if retryCount == 0 {
		retryCount = 1
	}
	time.Sleep(exponentialBackoffValue(retryCount))
}

// DriverName is the public name of the driver
const DriverName = "rdsmysql"

func init() {
	profileReg = make(map[string]*tls.Config)
	sql.Register(DriverName, &db{})
}
