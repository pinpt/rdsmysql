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

## Overview

The goal of this driver is to support Amazon RDS load balancing for MySQL and is most effective when RDS is configured to auto scale in and out.

Queries are transparently split to the master vs the replica based on the method used against the sql.DB.

All `Exec`, `ExecContext` or `Begin` transactions will be issued against the master host.

All `Query`, `QueryRow`, `QueryContext`, `QueryRowContext` queries will be issued against a replica picked at random (if more than 1 replica available).

When the Database is opened, the driver will (in the background) monitor replica changes in RDS using the information_schema.replica_host_status table.  When RDS is setup to auto scale in and out, the driver will keep track of these changes and load balance queries across them as the replicas change.  If no replica is available, the master host will be used instead.

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
