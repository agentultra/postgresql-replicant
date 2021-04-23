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

import Database.PostgreSQL.Replicant.Message
import Database.PostgreSQL.Replicant.PostgresUtils
import Database.PostgreSQL.Replicant.State
import Database.PostgreSQL.Replicant.Util
import Database.PostgreSQL.Replicant.Types.Lsn
import Database.PostgreSQL.Replicant.Queue (FifoQueue)
import qualified Database.PostgreSQL.Replicant.Queue as Q

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
    Nothing -> pure Nothing
    Just r  -> do
      systemId <- getvalue' r (toRow 0) (toColumn 0)
      timeline <- getvalue' r (toRow 0) (toColumn 1)
      logpos   <- getvalue' r (toRow 0) (toColumn 2)
      dbname   <- getvalue' r (toRow 0) (toColumn 3)
      case (systemId, timeline, logpos, dbname) of
        (Just s, Just t, Just l, d) -> do
          case fromByteString l of
            Left _ -> pure Nothing
            Right logPosLsn ->
              pure $ Just (IdentifySystem s t logPosLsn d)
        _ -> pure Nothing

data ReplicationSlot =
  ReplicationSlot
  { replicationSlotName            :: ByteString
  , replicationSlotConsistentPoint :: ByteString
  , replicationSlotSnapshotName    :: ByteString
  , replicationSlotOutputPlugin    :: ByteString
  }
  deriving (Eq, Show)

createReplicationSlotCommand :: ByteString -> ByteString
createReplicationSlotCommand slotName =
  B.intercalate " " ["CREATE_REPLICATION_SLOT", slotName, "LOGICAL wal2json"]

-- | Create a replication slot using synchronous query execution.
-- @Nothing@ means the command was unsuccessful and the slot was not
-- created.
createReplicationSlotSync :: Connection -> ByteString -> IO (Maybe ReplicationSlot)
createReplicationSlotSync conn slotName = do
  result <- exec conn $ createReplicationSlotCommand slotName
  case result of
    Nothing -> pure Nothing
    Just r  -> do
      sName           <- getvalue' r (toRow 0) (toColumn 0)
      consistentPoint <- getvalue' r (toRow 0) (toColumn 1)
      snapshotName    <- getvalue' r (toRow 0) (toColumn 2)
      outputPlugin    <- getvalue' r (toRow 0) (toColumn 3)
      case (sName, consistentPoint, snapshotName, outputPlugin) of
        (Just s, Just c, Just sn, Just op) ->
          pure $ Just (ReplicationSlot s c sn op)
        _ -> pure Nothing

startReplicationCommand :: ByteString -> LSN -> ByteString
startReplicationCommand slotName systemLogPos =
  B.intercalate " " ["START_REPLICATION SLOT", slotName, "LOGICAL", (toByteString systemLogPos)]

-- | This thread handles the COPY OUT mode messages.  PostgreSQL uses
-- this mode to copy the data from a WAL log file to the socket in the
-- streaming replication protocol.
handleCopyOutData
  :: Chan PrimaryKeepAlive
  -> WalProgressState
  -> Connection
  -> (Change -> IO a)
  -> IO ()
handleCopyOutData chan walState conn cb = forever $ do
  d <- getCopyData conn False
  case d of
    CopyOutRow row    -> handleReplicationRow chan walState conn row cb
    CopyOutWouldBlock -> handleReplicationWouldBlock conn
    CopyOutDone       -> handleReplicationDone conn
    CopyOutError      -> handleReplicationError conn

handleReplicationRow
  :: Chan PrimaryKeepAlive
  -> WalProgressState
  -> Connection
  -> ByteString
  -> (Change -> IO a)
  -> IO ()
handleReplicationRow keepAliveChan walState _ row cb =
  case decode @WalCopyData row of
    Left err ->
      throwIO
      $ ReplicantException
      $ "handleReplicationRow (decode error): " ++ err
    Right m  -> case m of
      XLogDataM xlog -> case eitherDecode' @Change $ BL.fromStrict $ xLogDataWalData xlog of
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
      KeepAliveM keepAlive -> writeChan keepAliveChan keepAlive

-- | We're not expecting to write any data when reading the
-- replication stream from this thread, so this is a no-op.
handleReplicationWouldBlock :: Connection -> IO ()
handleReplicationWouldBlock _ = pure ()

handleReplicationDone :: Connection -> IO ()
handleReplicationDone _ = do
  putStrLn "We should be finished with copying?"
  pure ()

data ReplicantException = ReplicantException String
  deriving (Show, Typeable)

instance Exception ReplicantException

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
startReplicationStream :: Connection -> ByteString -> LSN -> (Change -> IO a) -> IO ()
startReplicationStream conn slotName systemLogPos cb = do
  let initialWalProgress = WalProgress systemLogPos systemLogPos systemLogPos
  walProgressState <- WalProgressState <$> newMVar initialWalProgress
  result <- exec conn $ startReplicationCommand slotName systemLogPos
  case result of
    Nothing -> putStrLn "Woopsie" >>= \_ -> pure ()
    Just r  -> do
      status <- resultStatus r
      case status of
        CopyBoth -> do
          keepAliveChan <- newChan
          statusUpdateQueue <- Q.empty
          race
            (keepAliveHandler conn keepAliveChan walProgressState statusUpdateQueue)
            (handleCopyOutData keepAliveChan walProgressState conn cb)
            `catch`
            \exc -> do
              finish conn
              throwIO @SomeException exc
          return ()
        _ -> do
          err <- errorMessage conn
          print err

-- | This thread listens on the channel for /primary keep-alive
-- messages/ from the server and responds to them with the /update
-- status/ message using the current WAL stream state.
keepAliveHandler :: Connection -> Chan PrimaryKeepAlive -> WalProgressState -> FifoQueue StandbyStatusUpdate -> IO ()
keepAliveHandler conn msgs (WalProgressState walState) statusUpdateQueue = forever $ do
  isNull <- Q.null statusUpdateQueue
  when (not isNull) $ sendQueuedUpdates conn statusUpdateQueue
  keepAlive <- readChan msgs
  (WalProgress received flushed applied) <- readMVar walState
  case primaryKeepAliveResponseExpectation keepAlive of
    DoNotRespond -> do
      putStrLn "DoNotRespond"
      threadDelay 1000
    ShouldRespond -> do
      timestamp <- postgresEpoch
      let statusUpdate =
            StandbyStatusUpdate
            received
            flushed
            applied
            timestamp
            DoNotRespond
      print $ "statusUpdate is " ++ show statusUpdate
      copyResult <- putCopyData conn $ encode statusUpdate
      case copyResult of
        CopyInOk -> do
          putStrLn "CopyInOk!"
        CopyInError -> do
          err <- errorMessage conn
          print err
        CopyInWouldBlock -> do
          Q.enqueue statusUpdateQueue statusUpdate
          threadDelay 500

-- | Drain the queue of StandbyStatusUpdates and send them to the
-- server.  Keep retrying (with delay) until the queue is empty.
--
-- The queue should be non-empty and this operation could block.
sendQueuedUpdates :: Connection -> FifoQueue StandbyStatusUpdate -> IO ()
sendQueuedUpdates conn statusUpdateQueue = do
  maybeUpdate <- Q.dequeue statusUpdateQueue
  case maybeUpdate of
    Nothing -> pure ()
    Just update -> do
      copyResult <- putCopyData conn $ encode update
      case copyResult of
        CopyInOk -> do
          threadDelay 500
          sendQueuedUpdates conn statusUpdateQueue
        CopyInError -> do
          err <- maybe "sendQueuedUpdates: unknown error" id <$> errorMessage conn
          throwIO $ ReplicantException $ B.unpack err
        CopyInWouldBlock -> do
          Q.enqueueRight statusUpdateQueue update
          threadDelay 500
          sendQueuedUpdates conn statusUpdateQueue
