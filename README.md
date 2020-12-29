# postgres-replicant

A PostgreSQL streaming logical replication client for Haskell.

React to database changes in real-time.  Enrich data, send
notifications, monitor suspicious activity, etc.

## FAQ

### Why not use LISTEN/NOTIFY

You don't have to set up triggers for each table with logical
replication.  You also don't have the message size limitation.  That
limitation often forces clients to perform a query for each message to
fetch the data.  We only use one connection.
