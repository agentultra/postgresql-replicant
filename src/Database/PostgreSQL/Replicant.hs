{-# LANGUAGE RecordWildCards #-}
module Database.PostgreSQL.Replicant
    ( withLogicalStream
    , PgSettings (..)
    ) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Maybe (fromMaybe)
import Database.PostgreSQL.LibPQ
import GHC.Event
import Network.Socket.KeepAlive
import System.Posix.Types

import Database.PostgreSQL.Replicant.Protocol
import Database.PostgreSQL.Replicant.Message
import Database.PostgreSQL.Replicant.ReplicationSlot

data PgSettings
  = PgSettings
  { pgUser     :: String
  , pgDbName   :: String
  , pgHost     :: String
  , pgPort     :: String
  , pgSlotName :: String
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

withLogicalStream :: PgSettings -> (Change -> IO a) -> IO ()
withLogicalStream settings cb = do
  conn <- connectStart $ pgConnectionString settings
  mFd <- socket conn
  let sockFd = fromMaybe (error "Failed getting socket file descriptor") mFd
  pollResult <- pollConnectStart conn sockFd
  case pollResult of
    PollingFailed -> error "Unable to connect to the database"
    PollingOk -> do
      putStrLn "Connection ready!"
      (Just info) <- identifySystemSync conn
      (Just repSlot) <- setupReplicationSlot conn $ B.pack . pgSlotName $ settings
      startReplicationStream conn (slotName repSlot) (identifySystemLogPos info) cb
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
