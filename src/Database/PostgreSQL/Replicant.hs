{-# LANGUAGE RecordWildCards #-}

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
    , PgSettings (..)
    ) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Text.Read as T
import Database.PostgreSQL.LibPQ
import GHC.Event
import Network.Socket.KeepAlive
import System.Posix.Types

import Database.PostgreSQL.Replicant.Exception
import Database.PostgreSQL.Replicant.Protocol
import Database.PostgreSQL.Replicant.Message
import Database.PostgreSQL.Replicant.ReplicationSlot
import Database.PostgreSQL.Replicant.Util
import Database.PostgreSQL.Replicant.Types.Lsn

data PgSettings
  = PgSettings
  { pgUser        :: String
  , pgDbName      :: String
  , pgHost        :: String
  , pgPort        :: String
  , pgSlotName    :: String
  , pgUpdateDelay :: String -- ^ Controls how frequently the
                            -- primaryKeepAlive thread updates
                            -- PostgresSQL in @ms@
  }
  deriving (Eq, Show)

pgConnectionString :: PgSettings -> ByteString
pgConnectionString PgSettings {..} = B.intercalate " "
  [ "user=" <> (B.pack pgUser)
  , "dbname=" <> (B.pack pgDbName)
  , "host=" <> (B.pack pgHost)
  , "port=" <> (B.pack pgPort)
  , "replication=database"
  ]

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
withLogicalStream :: PgSettings -> (Change -> IO a) -> IO ()
withLogicalStream settings cb = do
  conn <- connectStart $ pgConnectionString settings
  mFd <- socket conn
  sockFd <- maybeThrow (ReplicantException "withLogicalStream: could not get socket fd") mFd
  pollResult <- pollConnectStart conn sockFd
  let updateFreq = getUpdateDelay settings
  case pollResult of
    PollingFailed -> throwIO $ ReplicantException "withLogicalStream: Unable to connect to the database"
    PollingOk -> do
      maybeInfo <- identifySystemSync conn
      info <- maybeThrow (ReplicantException "withLogicalStream: could not get system information") maybeInfo
      repSlot <- setupReplicationSlot conn $ B.pack . pgSlotName $ settings
      startReplicationStream conn (slotName repSlot) (identifySystemLogPos info) updateFreq cb
      pure ()
  where
    pollConnectStart :: Connection -> Fd -> IO PollingStatus
    pollConnectStart conn fd@(Fd cint) = do
      pollStatus <- connectPoll conn
      case pollStatus of
        PollingReading -> do
          threadWaitRead fd
          pollConnectStart conn fd
        PollingWriting -> do
          threadWaitWrite fd
          pollConnectStart conn fd
        PollingOk -> do
          keepAliveResult <- setKeepAlive cint $ KeepAlive True 60 2
          pure PollingOk
        PollingFailed -> pure PollingFailed
    getUpdateDelay :: PgSettings -> Int
    getUpdateDelay PgSettings {..} =
      case T.decimal . T.pack $ pgUpdateDelay of
        Left _ -> 3000
        Right (i, _) -> i
