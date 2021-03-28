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

Currently SSL is not supported but is high-priority.  It is
recommended you turn `ssl = off` in your `postgresql.conf` settings
until support is added.

### WAL Output Plugins

Presently we only support the `wal2json` WAL output plugin and only
the v1 format.  Support for the v2 format is planned as are more
output plugins.

## FAQ

### Why not use LISTEN/NOTIFY

You don't have to set up triggers for each table with logical
replication.  You also don't have the message size limitation.  That
limitation often forces clients to perform a query for each message to
fetch the data.  We only use one connection.
