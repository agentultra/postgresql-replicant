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
