module Database.PostgreSQL.Replicant.State where

import Control.Concurrent
import GHC.Int

data WalProgress
  = WalProgress
  { walProgressReceived :: Int64
  , walProgressFlushed  :: Int64
  , walProgressApplied  :: Int64
  }
  deriving (Eq, Show)

newtype WalProgressState = WalProgressState (MVar WalProgress)

updateWalProgress :: WalProgressState -> Int64 -> IO ()
updateWalProgress (WalProgressState state) bytesReceived = do
  walProgress <- takeMVar state
  putMVar state
    $ walProgress { walProgressReceived = walProgressReceived walProgress + bytesReceived
                  , walProgressFlushed  = walProgressFlushed walProgress + bytesReceived
                  , walProgressApplied  = walProgressApplied walProgress + bytesReceived
                  }
