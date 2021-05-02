{-|
Module      : Database.PostgreSQL.Replicant.State
Description : Internal replication stream state
Copyright   : (c) James King, 2020, 2021
License     : BSD3
Maintainer  : james@agentultra.com
Stability   : experimental
Portability : POSIX

This module has the types and functions for maintaining the client
stream state.

After initiating a replication stream the wal_sender process on the
server may require clients to periodically send progress updates.  The
wal_sender process uses those updates to maintain its internal view of
the clients' state.

This enables the server to report on things like replication lag and
enables the client to disconnect and restart the stream where it left
off.
-}
module Database.PostgreSQL.Replicant.State where

import Control.Concurrent
import GHC.Int
import Database.PostgreSQL.Replicant.Types.Lsn

data WalProgress
  = WalProgress
  { walProgressReceived :: LSN
  , walProgressFlushed  :: LSN
  , walProgressApplied  :: LSN
  }
  deriving (Eq, Show)

newtype WalProgressState = WalProgressState (MVar WalProgress)

updateWalProgress :: WalProgressState -> Int64 -> IO ()
updateWalProgress (WalProgressState state) bytesReceived = do
  walProgress <- takeMVar state
  putMVar state
    $ walProgress { walProgressReceived = walProgressReceived walProgress `add` bytesReceived
                  , walProgressFlushed  = walProgressFlushed walProgress `add` bytesReceived
                  , walProgressApplied  = walProgressApplied walProgress `add` bytesReceived
                  }
