# postgres-replicant

A PostgreSQL streaming logical replication client for Haskell.

React to database changes in real-time.  Enrich data, send
notifications, monitor suspicious activity, etc.

## Setup

In order to use a logical replication client you need to set the
`wal_level` in Postgres to `logical` in this case.  You can set that,
for example, in your `postgres.conf` file:

  wal_level = logical

Then restart your server, log in as `postgres` or another super user
and check:

  SHOW wal_level

It should show _logical_.

You will also need a user who is
[allowed](https://www.postgresql.org/docs/9.1/sql-alterrole.html
"PostgreSQL user role documentation") to use replication features.

### TODO: WAL Output Plugins

We may require something like `wal2json` or something to work.

## FAQ

### Why not use LISTEN/NOTIFY

You don't have to set up triggers for each table with logical
replication.  You also don't have the message size limitation.  That
limitation often forces clients to perform a query for each message to
fetch the data.  We only use one connection.
