# postgres-replicant

A PostgreSQL streaming logical replication client for Haskell.

React to database changes in real-time.

This library is currently _EXPERIMENTAL_ and is not ready for
production yet.  Developers are encouraged to test out the library,
add issues, contribute and make it awesome.

## Setup

In order to use a logical replication client you need to set the
`wal_level` in Postgres to `logical`.  You can set that, for example,
in your `postgres.conf` file:

    wal_level = logical

Then restart your server, log in as `postgres` or another super user
and check:

    SHOW wal_level

It should show _logical_.

You will also need a user who is
[allowed](https://www.postgresql.org/docs/9.1/sql-alterrole.html
"PostgreSQL user role documentation") to use replication features.

Then add a database and your user with the `REPLICATION` trait to the
database, grant all on it if you need to.

We haven't added authorization support yet to the library so make sure
in `pg_hba.conf` you `trust` local connections:

    host    all             all             127.0.0.1/32            trust

Caveat emptor.

You will also need to install the `wal2json` plugin for your
PostgreSQL server.  For example in Ubuntu-like distributions you can:

    $ sudo apt install postgresql-12-wal2json

Assuming you have installed PostgreSQL from the package repositories.

You can then setup a basic "hello world" program using this library:

``` haskell
module Main where

import Control.Exception

import Database.PostgreSQL.Replicant

main :: IO ()
main = do
  let settings = PgSettings "my-user" "my-database" "localhost" "5432" "testing"
  withLogicalStream settings $ \change -> do
    print change
      `catch` \err ->
      print err
```

Which should connect, create a `testing` replication slot, and start
sending your callback the changes to your database as they arrive.

### WAL Output Plugins

Presently we only support the `wal2json` WAL output plugin and only
the v1 format.  Support for the v2 format is planned as are more
output plugins.

## Example Program

Included is a simple program, `replicant-example` which should help
you test your connection settings and make sure that you can connect
and receive WAL changes.

You can change the connection settings through environment variables:

    PG_USER=myuser PG_DATABASE=mydb replicant-example

The configuration settings are:

- `PG_USER`: The replication user to connect as
- `PG_DATABASE`: The database to connect to
- `PG_HOST`: The host name of the database to connect to
- `PG_PORT`: The port to connect on
- `PG_SLOTNAME`: The replication slot to create or connect to
- `PG_UPDATEDELAY`: The frequency (in _ms_) to update PostgreSQL on
  the stream status

## FAQ

### Why not use LISTEN/NOTIFY

You don't have to set up triggers for each table with logical
replication.  You also don't have the message size limitation.  That
limitation often forces clients to perform a query for each message to
fetch the data and can use more than one connection.  We only use one
connection.

### What are the trade offs?

This library uses logical replication slots.  If your program loses
the connection to the database and cannot reconnect the server will
hold onto WAL files until your program can reconnect and resume the
replication stream.  On a busy database, depending on your
configuration, this can eat up disk space quickly.

It's a good idea to monitor your replication slots.  If
`postgresql-replicant` cannot reconnect to the slot and resume the
stream you may need to have a strategy for dropping the slot and
recreating it once conditions return to normal.  Have a strategy.
