{-|
Module      : Database.PostgreSQL.Replicant
Description : A PostgreSQL streaming replication library
Copyright   : (c) James King, 2020, 2021
License     : BSD3
Maintainer  : james@agentultra.com
Stability   : experimental
Portability : POSIX

Connect to a PostgreSQL server as a logical replication client and
receive changes.

The basic API is this:

@
  withLogicalStream defaultSettings $ \change -> do
    print change
    `catch` \err -> do
      show err
@

This is a low-level library meant to give the primitives necessary to
library authors to add streaming replication support.  The API here to
rather simplistic but should be hooked up to something like conduit to
provide better ergonomics.
-}

module Database.PostgreSQL.Replicant
    ( withLogicalStream
    -- * Types
    , PgSettings (..)
    -- * Re-exports
    , changeNextLSN
    ) where

import Control.Concurrent
import Control.Exception
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.Text as T
import qualified Data.Text.Read as T
import Database.PostgreSQL.LibPQ

import Database.PostgreSQL.Replicant.Connection
import Database.PostgreSQL.Replicant.Exception
import Database.PostgreSQL.Replicant.Protocol
import Database.PostgreSQL.Replicant.Message
import Database.PostgreSQL.Replicant.ReplicationSlot
import Database.PostgreSQL.Replicant.Settings
import Database.PostgreSQL.Replicant.Types.Lsn
import Database.PostgreSQL.Replicant.Util

-- | Connect to a PostgreSQL database as a user with the replication
-- attribute and start receiving changes using the logical replication
-- protocol.  Logical replication happens at the query level so the
-- changes you get represent the set of queries in a transaction:
-- /insert/, /update/, and /delete/.
--
-- This function will create the replication slot, if it doesn't
-- exist, or reconnect to it otherwise and restart the stream from
-- where the replication slot left off.
--
-- This function can throw exceptions in @IO@ and shut-down the
-- socket in case of any error.
withLogicalStream :: PgSettings -> (Change -> IO LSN) -> IO ()
withLogicalStream settings cb = do
  conn <- connect settings
  let updateFreq = getUpdateDelay settings
  maybeInfo <- identifySystemSync conn
  _ <- maybeThrow (ReplicantException "withLogicalStream: could not get system information") maybeInfo
  repSlot <- setupReplicationSlot conn $ B.pack . pgSlotName $ settings
  startReplicationStream conn (slotName repSlot) (slotRestart repSlot) updateFreq cb
  pure ()
  where
    getUpdateDelay :: PgSettings -> Int
    getUpdateDelay PgSettings {..} =
      case T.decimal . T.pack $ pgUpdateDelay of
        Left _ -> 3000
        Right (i, _) -> i
