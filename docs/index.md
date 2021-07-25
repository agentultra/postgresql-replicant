## Introduction ##

`postgresql-replicant` is a library for Haskell that understands
PostgreSQL's logical replication protocol and can stream changes that
happen in your database to your Haskell programs.

Let's say you work on an application built on top of PostgreSQL.
You've made sure your schema have appropriate `updated_at` and
`created_at` columns.  And then there comes an error report that
requires you to figure out when a value in a row in one of your tables
_changed_.  The problem is that this event happened ages ago.  The row
has definitely been updated since.

Maybe you have implemented _history_ tables and have trigger functions
to track these changes over time.

Perhaps you have a web application front-end that wants to receive
real-time updates to the data models so that users can react to
important information in your system.

Maybe you need to scale out your data processing, caching and
compliance pipelines and want to minimize the impact on your
application architecture.

All of these scenarios and more can be solved with
`postgresql-replicant`.

## Streaming Replication ##

PostgeSQL long ago implemented a feature that enables it to copy data
from one publishing server to a subscribing server.  This feature
streams these changes as they are applied on the publisher to the
subscriber.  It is called _streaming replication_ and is the feature
that `postgresql-replicant` exploits.

PostgreSQL offers two kinds of streaming replication: _logical_ and
_physical_.  For now `postgresql-replicant` is only concerned with
_logical replication_.  In this form of replication data is copied by
streaming the changes that happen at the level of database objects and
the statements that modify them.  It essentially streams the `insert`,
`update` and `delete` commands that make up each transaction.

_Physical replication_ on the other hand streams the raw chunks of
bytes that make up the data on the servers' primary storage.

### Changes ###

A program using this library will be mostly concerned with the
`Change` type:

    data Change
    = Change
    { changeNextLSN :: LSN
    , changeDeltas  :: [WalLogData]
    }
    deriving (Eq, Generic, Show)

A value of this type represents a _transaction_ that occurred on the
publishing server.  The list of `WalLogData` values represent the
statements that were applied to the publishing server in the
transaction.  Each change appears in the list in the same order as
they were applied on the server.

You can think of the `LSN` type as a _pointer_ into the publishing
stream.  If your program is disconnected the server will begin storing
the changes.  Once your program reconnects `postgresql-replicant` will
query the replication slot and ask to start the stream where it left
off.  This way your program should not miss a change.

Once you have consumed the change and applied it you return the `LSN`
value given in the `Change` to `postgresql-replicant`.  This will
update the server on your program's location in the stream.

### Replication Slots ###

In order to consume a replication stream a client creates a
_replication slot_ on the PostgreSQL publishing server.  The server
uses this slot to track the state of the client and to queue up
changes for the client should they lose their connection.

`postgresql-replicant` can handle creating, querying, and connecting
to a replication slot.  You supply a name for the slot in the
connection settings and `postgresql-replicant` can take care of the
rest.  Slot names must be unique and can contain only the same
characters as table names.

You can query the state of the replication slot created by
`postgresql-replicant` using this query:

    select * from pg_replication_slots where slot_name = 'my-slot';

### Connections ###

`postgresql-replicant` creates a `ReplicantConnection` type which is a
new-type wrapper around `postgresql-libpq` `Connection`.  The reason
for this is that the connection created by this library enables it to
send the replication commands that are not part of standard SQL.

Non-trivial programs will likely want to manage the underlying
connection to provide resource management, connection pools, and logic
to retry failed connections, etc.  It is best to use the `connect`
function to create the initial connection and manage it separately
from regular `Connection` values.  It is important to not wrap a
non-replication connection in a `ReplicantConnection` as this will
likely fail and throw an exception as soon as a replication command is
sent over the socket.

## How To Read this Library ##

The primary entry-point `postgresql-replicant` is
`src/Database/PostgreSQL/Replicant.hs`.  This is the natural place to
begin understanding how this library is structured.  A user of this
library should ideally only have to import this module and have all of
the tools they need to use the library _as intended_.

We try to use explicit export lists as infrequently as possible so
that users can have access to internal types, functions, etc for
whatever reason when integrating `postgresql-replicant` into their
program.  We have used them in modules in order to enforce a
module-level invariant on a type constructor, for example:
`src/Database/PostgreSQL/Replicant/Connection.hs`.  A user could not
create a value of `ReplicantConnection` with an arbitrary
`Connection`.  Only properly configured `Connection` values can be
used.  Improperly constructed `Connection` objects would result in
run-time exceptions.  Means to access the internals of
`ReplicantConnection` are still provided.  The idea is to use the
feature sparingly.

The general layout for the rest of the modules is to encapsulate a
single responsibility or concept each:

* `Connection.hs`: create connections
* `Exception.hs`: library exceptions
* `Message.hs`: the protocol message types, parsers, and serializers.
* `PostgresUtils.hs`: PostgreSQL-specific helper functions
* `Protocol.hs`: handle the streaming protocol
* `ReplicationSlot.hs`: create, query, connect to replication slots
* `Serialize.hs`: serialization/parsing helpers
* `State.hs`: manage client stream state
* `types/Lsn.hs`: the LSN PostgreSQL type
* `Util.hs`: Miscellaneous functions, combinators, etc.

The `Replicant.hs` module re-exports internals taken from these
modules to give the library its API.  The most important of these is
`Protocol.hs` and `Message.hs` which work in tandem.  `Protocol.hs`
handles the replication protocol which, when invoked with
`startReplicationStream` will race two threads:

* Keep Alive Handler: will periodically send status updates to the
  publishing server and respond to requests for updates from the
  server
* Copy Out Data Handler: will handle the stream data from the server
  and call the users' callback function to send them the `Change`
  values.

If either thread should throw an exception in `IO` the underlying
connection will be shutdown and the exception re-thrown to the user.

## Cookbook ##

Coming soon
