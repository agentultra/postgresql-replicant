{-# LANGUAGE RecordWildCards #-}
module Lib
    ( withConnection
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

withConnection :: PgSettings -> IO ()
withConnection settings = do
  conn <- connectStart $ pgConnectionString settings
  mFd <- socket conn
  let sockFd = fromMaybe (error "Failed getting socket file descriptor") mFd
  pollResult <- pollConnectStart conn sockFd
  case pollResult of
    PollingFailed -> error "Unable to connect to the database"
    PollingOk -> do
      putStrLn "Connection ready!"
      (Just info) <- identifySystemSync conn
      (Just repSlot) <- createReplicationSlotSync conn $ B.pack . pgSlotName $ settings
      startReplicationStream conn (replicationSlotName repSlot) (identifySystemLogPos info)
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
