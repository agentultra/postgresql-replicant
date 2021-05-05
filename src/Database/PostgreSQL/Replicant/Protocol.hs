{-|
Module      : Database.PostgreSQL.Replicant.Protocol
Description : Streaming replication protocol
Copyright   : (c) James King, 2020, 2021
License     : BSD3
Maintainer  : james@agentultra.com
Stability   : experimental
Portability : POSIX

This module implements the Postgres streaming replication protocol.

See: https://www.postgresql.org/docs/9.5/protocol-replication.html
-}
module Database.PostgreSQL.Replicant.Protocol where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.Chan
import Control.Exception.Base
import Control.Monad (forever, when)
import Data.Aeson (eitherDecode')
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as B
import Data.Sequence (Seq, ViewL (..), (<|))
import qualified Data.Sequence as S
import Data.Serialize
import Data.Typeable
import Database.PostgreSQL.LibPQ

import Database.PostgreSQL.Replicant.Exception
import Database.PostgreSQL.Replicant.Message
import Database.PostgreSQL.Replicant.PostgresUtils
import Database.PostgreSQL.Replicant.State
import Database.PostgreSQL.Replicant.Util
import Database.PostgreSQL.Replicant.Types.Lsn
import Database.PostgreSQL.Replicant.ReplicationSlot

-- | The information returned by the @IDENTIFY_SYSTEM@ command
-- establishes the stream's log start, position, and information about
-- the database.
data IdentifySystem
  = IdentifySystem
  { identifySystemSytemId  :: ByteString
  , identifySystemTimeline :: ByteString
  , identifySystemLogPos   :: LSN
  , identifySystemDbName   :: Maybe ByteString
  }
  deriving (Eq, Show)

identifySystemCommand :: ByteString
identifySystemCommand = "IDENTIFY_SYSTEM"

-- | Synchronously execute the @IDENTIFY SYSTEM@ command which returns
-- some basic system information about the server.
identifySystemSync :: Connection -> IO (Maybe IdentifySystem)
identifySystemSync conn = do
  result <- exec conn identifySystemCommand
  case result of
    Just r -> do
      resultStatus <- resultStatus r
      case resultStatus of
        TuplesOk -> do
          systemId <- getvalue' r (toRow 0) (toColumn 0)
          timeline <- getvalue' r (toRow 0) (toColumn 1)
          logpos   <- getvalue' r (toRow 0) (toColumn 2)
          dbname   <- getvalue' r (toRow 0) (toColumn 3)
          case (systemId, timeline, logpos, dbname) of
            (Just s, Just t, Just l, d) -> do
              case fromByteString l of
                Left _ -> pure Nothing
                Right logPosLsn -> do
                  pure $ Just (IdentifySystem s t logPosLsn d)
            _ -> pure Nothing
        _ -> do
          err <- maybe "identifySystemSync: unknown error" id <$> errorMessage conn
          throwIO $ ReplicantException (B.unpack err)
    _ -> do
      err <- maybe "identifySystemSync: unknown error" id <$> errorMessage conn
      throwIO $ ReplicantException (B.unpack err)

-- | Create a @START_REPLICATION_SLOT@ query, escaping the slot name
-- passed in by the user.
startReplicationCommand :: Connection -> ByteString -> LSN -> IO ByteString
startReplicationCommand conn slotName systemLogPos = do
  escapedName <- escapeIdentifier conn slotName
  case escapedName of
    Nothing -> throwIO $ ReplicantException $ "Invalid slot name: " ++ show slotName
    Just escaped ->
      pure $
      B.intercalate
      ""
      [ "START_REPLICATION SLOT "
      , escaped
      , " LOGICAL "
      , (toByteString systemLogPos)
      ]

-- | This handles the COPY OUT mode messages.  PostgreSQL uses this
-- mode to copy the data from a WAL log file to the socket in the
-- streaming replication protocol.
handleCopyOutData
  :: WalProgressState
  -> Connection
  -> (Change -> IO a)
  -> IO ()
handleCopyOutData walState conn cb = forever $ do
  d <- getCopyData conn False
  case d of
    CopyOutRow row    -> handleReplicationRow walState conn row cb
    CopyOutWouldBlock -> handleReplicationWouldBlock conn
    CopyOutDone       -> handleReplicationDone conn
    CopyOutError      -> handleReplicationError conn

handleReplicationRow
  :: WalProgressState
  -> Connection
  -> ByteString
  -> (Change -> IO a)
  -> IO ()
handleReplicationRow walState _ row cb =
  case decode @WalCopyData row of
    Left err ->
      throwIO
      $ ReplicantException
      $ "handleReplicationRow (decode error): " ++ err
    Right (XLogDataM xlog) ->
      case eitherDecode' @Change $ BL.fromStrict $ xLogDataWalData xlog of
        Left err ->
          throwIO
          $ ReplicantException
          $ "handleReplicationRow (parse error): " ++ err
        Right walLogData -> do
          let logStart      = xLogDataWalStart xlog
              logEnd        = xLogDataWalEnd xlog
              bytesReceived = logEnd `subLsn` logStart
          _ <- updateWalProgress walState bytesReceived
          _ <- cb walLogData
          pure ()
    _ -> pure ()

-- | We're not expecting to write any data when reading the
-- replication stream from this thread, so this is a no-op.
handleReplicationWouldBlock :: Connection -> IO ()
handleReplicationWouldBlock _ = pure ()

-- | TODO: When the @COPY OUT@ mode is finished the server sends one
-- result tuple with the information to start streaming the next WAL
-- file _when_ the stream was started in an earlier timeline.
handleReplicationDone :: Connection -> IO ()
handleReplicationDone _ = pure ()

-- | Used to re-throw an exception received from the server.
handleReplicationError :: Connection -> IO ()
handleReplicationError conn = do
  err <- errorMessage conn
  throwIO (ReplicantException $ B.unpack . maybe "Unknown error" id $ err)
  pure ()

-- | Initiate the streaming replication protocol handler.  This will
-- race the /keep-alive/ and /copy data/ handler threads.  It will
-- catch and rethrow exceptions from either thread if any fails or
-- returns.
startReplicationStream :: Connection -> ByteString -> LSN -> Int -> (Change -> IO a) -> IO ()
startReplicationStream conn slotName systemLogPos updateFreq cb = do
  let initialWalProgress = WalProgress systemLogPos systemLogPos systemLogPos
  walProgressState <- WalProgressState <$> newMVar initialWalProgress
  replicationCommandQuery <- startReplicationCommand conn slotName systemLogPos
  result <- exec conn replicationCommandQuery
  case result of
    Nothing -> do
      err <- maybe "startReplicationStream: unable to start stream" id <$> errorMessage conn
      throwIO $ ReplicantException $ B.unpack err
    Just r  -> do
      status <- resultStatus r
      case status of
        CopyBoth -> do
          race
            (keepAliveHandler conn walProgressState updateFreq)
            (handleCopyOutData walProgressState conn cb)
            `catch`
            \exc -> do
              finish conn
              throwIO @SomeException exc
          return ()
        _ -> do
          err <- errorMessage conn
          print err

-- | This function repeatedly sends `StandbyStatusUpdate` messages to
-- the publishing server.  This keeps the wal sender process on the
-- PostgreSQL server from timing out.
keepAliveHandler :: Connection -> WalProgressState -> Int -> IO ()
keepAliveHandler conn (WalProgressState walState) updateFreq = forever $ do
  (WalProgress received flushed applied) <- readMVar walState
  timestamp <- postgresEpoch
  let statusUpdate =
        StandbyStatusUpdate
        received
        flushed
        applied
        timestamp
        DoNotRespond
  copyResult <- putCopyData conn $ encode statusUpdate
  case copyResult of
    CopyInOk -> do
      threadDelay updateFreq
    CopyInError -> do
      err <- maybe "keepAliveHandler: unknown CopyInError" id <$> errorMessage conn
      throwIO $ ReplicantException $ B.unpack err
    CopyInWouldBlock -> do
      threadDelay 500
