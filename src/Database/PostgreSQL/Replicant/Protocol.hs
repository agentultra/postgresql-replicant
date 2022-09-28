{-# LANGUAGE Strict #-}

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
import Control.Concurrent.STM
import Control.Exception.Base
import Control.Monad (forever)
import Data.Aeson (eitherDecode')
import Data.Binary
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as B
import Data.Maybe
import Database.PostgreSQL.LibPQ

import Database.PostgreSQL.Replicant.Connection
import Database.PostgreSQL.Replicant.Exception
import Database.PostgreSQL.Replicant.Message
import Database.PostgreSQL.Replicant.PostgresUtils
import Database.PostgreSQL.Replicant.State
import Database.PostgreSQL.Replicant.Types.Lsn

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
identifySystemSync :: ReplicantConnection -> IO (Maybe IdentifySystem)
identifySystemSync conn = do
  result <- exec (getConnection conn) identifySystemCommand
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
          err <- fromMaybe "identifySystemSync: unknown error"
            <$> errorMessage (getConnection conn)
          throwIO $ ReplicantException (B.unpack err)
    _ -> do
      err <- fromMaybe "identifySystemSync: unknown error"
        <$> errorMessage (getConnection conn)
      throwIO $ ReplicantException (B.unpack err)

-- | Create a @START_REPLICATION_SLOT@ query, escaping the slot name
-- passed in by the user.
startReplicationCommand :: ReplicantConnection -> ByteString -> LSN -> IO ByteString
startReplicationCommand conn slotName systemLogPos = do
  escapedName <- escapeIdentifier (getConnection conn) slotName
  case escapedName of
    Nothing -> throwIO $ ReplicantException $ "Invalid slot name: " ++ show slotName
    Just escaped ->
      pure $
      B.intercalate
      ""
      [ "START_REPLICATION SLOT "
      , escaped
      , " LOGICAL "
      , toByteString systemLogPos
      , " (\"include-lsn\" 'on')"
      ]

-- | This handles the COPY OUT mode messages.  PostgreSQL uses this
-- mode to copy the data from a WAL log file to the socket in the
-- streaming replication protocol.
handleCopyOutData
  :: TChan PrimaryKeepAlive
  -> WalProgressState
  -> ReplicantConnection
  -> (Change -> IO LSN)
  -> IO ()
handleCopyOutData chan walState conn cb = forever $ do
  d <- getCopyData (getConnection conn) False
  case d of
    CopyOutRow row -> handleReplicationRow chan walState conn row cb
    CopyOutError   -> handleReplicationError conn
    _              -> handleReplicationNoop

handleReplicationRow
  :: TChan PrimaryKeepAlive
  -> WalProgressState
  -> ReplicantConnection
  -> ByteString
  -> (Change -> IO LSN)
  -> IO ()
handleReplicationRow keepAliveChan walState _ row cb =
  case decodeOrFail @WalCopyData . BL.fromStrict $ row of
    Left (_, _, err) ->
      throwIO
      $ ReplicantException
      $ "handleReplicationRow (decode error): " ++ err
    Right (_, _, m) -> case m of
      XLogDataM xlog -> do
        case eitherDecode' @Change $ BL.fromStrict $ xLogDataWalData xlog of
          Left err ->
            throwIO
            $ ReplicantException
            $ "handleReplicationRow (parse error): " ++ err
          Right walLogData -> do
            consumedLSN <- cb walLogData
            updateWalProgress walState consumedLSN
      KeepAliveM keepAlive -> atomically $ writeTChan keepAliveChan keepAlive

-- | Used to re-throw an exception received from the server.
handleReplicationError :: ReplicantConnection -> IO ()
handleReplicationError conn = do
  err <- errorMessage $ getConnection conn
  throwIO (ReplicantException $ B.unpack . fromMaybe "Unknown error" $ err)
  pure ()

handleReplicationNoop :: IO ()
handleReplicationNoop = pure ()


-- | Initiate the streaming replication protocol handler.  This will
-- race the /keep-alive/ and /copy data/ handler threads.  It will
-- catch and rethrow exceptions from either thread if any fails or
-- returns.
startReplicationStream :: ReplicantConnection -> ByteString -> LSN -> Int -> (Change -> IO LSN) -> IO ()
startReplicationStream conn slotName systemLogPos _ cb = do
  let initialWalProgress = WalProgress systemLogPos systemLogPos systemLogPos
  walProgressState <- WalProgressState <$> newMVar initialWalProgress
  replicationCommandQuery <- startReplicationCommand conn slotName systemLogPos
  result <- exec (getConnection conn) replicationCommandQuery
  case result of
    Nothing -> do
      err <- fromMaybe "startReplicationStream: unknown error starting stream"
        <$> errorMessage (getConnection conn)
      throwIO $ ReplicantException $ "startReplicationStream: " ++ B.unpack err
    Just r  -> do
      status <- resultStatus r
      case status of
        CopyBoth -> do
          keepAliveChan <- atomically newTChan
          race
            (keepAliveHandler conn keepAliveChan walProgressState)
            (handleCopyOutData keepAliveChan walProgressState conn cb)
            `catch`
            \exc -> do
              finish (getConnection conn)
              throwIO @SomeException exc
          return ()
        _ -> do
          err <- fromMaybe "startReplicationStream: unknown error entering COPY mode" <$> errorMessage (getConnection conn)
          throwIO $ ReplicantException $ B.unpack err

-- | This listens on the channel for /primary keep-alive messages/
-- from the server and responds to them with the /update status/
-- message using the current WAL stream state.  It will attempt to
-- buffer prior update messages when the socket is blocked.
keepAliveHandler :: ReplicantConnection -> TChan PrimaryKeepAlive -> WalProgressState -> IO ()
keepAliveHandler conn msgs walProgressState = forever $ do
  mKeepAlive <- atomically $ tryReadTChan msgs
  case mKeepAlive of
    Nothing -> do
      sendStatusUpdate conn walProgressState
      threadDelay 3000000
    Just keepAlive' -> do
      case primaryKeepAliveResponseExpectation keepAlive' of
        DoNotRespond -> do
          threadDelay 1000
        ShouldRespond -> do
          sendStatusUpdate conn walProgressState

sendStatusUpdate
  :: ReplicantConnection
  -> WalProgressState
  -> IO ()
sendStatusUpdate conn w@(WalProgressState walState) = do
  (WalProgress received flushed applied) <- readMVar walState
  timestamp <- postgresEpoch
  let statusUpdate =
        StandbyStatusUpdate
        received
        flushed
        applied
        timestamp
        DoNotRespond
  copyResult <- putCopyData (getConnection conn) . BL.toStrict $ encode statusUpdate
  case copyResult of
    CopyInOk -> do
      flushResult <- flush (getConnection conn)
      case flushResult of
        FlushOk -> pure ()
        FlushFailed -> do
          err <- fromMaybe "sendStatusUpdate: error flushing message to server" <$> errorMessage (getConnection conn)
          throwIO $ ReplicantException $ B.unpack err
        FlushWriting -> tryAgain (getConnection conn) w
    CopyInError -> do
      err <- fromMaybe "sendStatusUpdate: unknown error sending COPY IN" <$> errorMessage (getConnection conn)
      throwIO $ ReplicantException $ B.unpack err
    CopyInWouldBlock -> tryAgain (getConnection conn) w
  where
    tryAgain c ws = do
      mSockFd <- socket c
      case mSockFd of
        Nothing ->
          throwIO $ ReplicantException "sendStatusUpdate: failed to get socket fd"
        Just sockFd -> do
          threadWaitWrite sockFd
          sendStatusUpdate conn ws
