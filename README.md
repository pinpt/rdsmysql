<div align="center">
	<img width="500" src=".github/logo.svg" alt="pinpt-logo">
</div>

<p align="center" color="#6a737d">
	<strong>Golang SQL driver for Amazon MySQL RDS which supports replica load balanacing</strong>
</p>

## Install

```
go get -u github.com/pinpt/rdsmysql
```

## Usage

Use it like a normal MySQL driver except instead of `mysql` use `rdsmysql` and use the `hostname` to the RDS cluster as the url.

For example, if your RDS cluster was `foo-bar-rds-cluster.cluster-acddd9x9x9.us-east-1.rds.amazonaws.com`, you would use:

```
db, err := sql.Open("rdsmysql", "myuser:somepass@foo-bar-rds-cluster.cluster-acddd9x9x9.us-east-1.rds.amazonaws.com:3306/mydb")
```

In the above example, we used the username `mysuser` with a password of `somepass` and selected the database named `mydb`.

You can add any additional mysql driver options as normal query parameters.  By default, TLS will be properly set to the RDS TLS certificates.

The following are optional query parameters to tune the driver:

- `max-time-leaving`: specifies how often the client should query the sql server for current servers list. defaults to 30s
- `fail-duration`: the delay after node was removed from replica_host_status or query failed and db.Close is called. Also required for long running queries to allow interrupting iterating rows. shorter time cleans up resources faster. longer time reuses db instance and connection for temporary errors. defaults to 180s
- `update-duration`: the duration the node is marked as failed after failing query. The node is ignored for this duration. shorter time re-connects faster for restarts and temporary errors. longer time avoids unnecessary re-tries for shutdowns. defaults to 240s

You can set debug logging by implementing the Logger interface and setting it to the `L` public package variable.

## License

All of this code is Copyright &copy; 2018 by PinPT, Inc. Licensed under the MIT License
