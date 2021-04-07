module Database.PostgreSQL.Replicant.Protocol where

import Control.Concurrent
import Control.Concurrent.Chan
import Control.Exception.Base
import Control.Monad (forever)
import Data.Aeson (eitherDecode')
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as B
import Data.Serialize
import Data.Typeable
import Database.PostgreSQL.LibPQ

import Database.PostgreSQL.Replicant.Message
import Database.PostgreSQL.Replicant.PostgresUtils
import Database.PostgreSQL.Replicant.State
import Database.PostgreSQL.Replicant.Util

data IdentifySystem
  = IdentifySystem
  { identifySystemSytemId  :: ByteString
  , identifySystemTimeline :: ByteString
  , identifySystemLogPos   :: ByteString
  , identifySystemDbName   :: Maybe ByteString
  }
  deriving (Eq, Show)

identifySystemCommand :: ByteString
identifySystemCommand = "IDENTIFY_SYSTEM"

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
        (Just s, Just t, Just l, d) -> pure $ Just (IdentifySystem s t l d)
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

startReplicationCommand :: ByteString -> ByteString -> ByteString
startReplicationCommand slotName systemLogPos =
  B.intercalate " " ["START_REPLICATION SLOT", slotName, "LOGICAL", systemLogPos]

handleCopyOutData :: Chan PrimaryKeepAlive -> WalProgressState -> Connection -> IO ()
handleCopyOutData chan walState conn = forever $ do
  d <- getCopyData conn False
  case d of
    CopyOutRow row    -> handleReplicationRow chan walState conn row
    CopyOutWouldBlock -> handleReplicationWouldBlock conn
    CopyOutDone       -> handleReplicationDone conn
    CopyOutError      -> handleReplicationError conn

handleReplicationRow :: Chan PrimaryKeepAlive -> WalProgressState -> Connection -> ByteString -> IO ()
handleReplicationRow keepAliveChan walState _ row =
  case decode @WalCopyData row of
    Left err -> print err
    Right m  -> case m of
      XLogDataM xlog -> case eitherDecode' @Change $ BL.fromStrict $ xLogDataWalData xlog of
        Left err -> putStrLn err
        Right walLogData -> do
          _ <- updateWalProgress walState (mkInt64 . B.length $ (xLogDataWalData xlog))
          print walLogData
      KeepAliveM keepAlive -> writeChan keepAliveChan keepAlive

handleReplicationWouldBlock :: Connection -> IO ()
handleReplicationWouldBlock _ = do
  putStrLn "What do here?"
  pure ()

handleReplicationDone :: Connection -> IO ()
handleReplicationDone _ = do
  putStrLn "We should be finished with copying?"
  pure ()

data ReplicantException = ReplicantException String
  deriving (Show, Typeable)

instance Exception ReplicantException

handleReplicationError :: Connection -> IO ()
handleReplicationError conn = do
  err <- errorMessage conn
  throwIO (ReplicantException $ B.unpack . maybe "Unknown error" id $ err)
  pure ()

startReplicationStream :: Connection -> ByteString -> ByteString -> IO ()
startReplicationStream conn slotName systemLogPos = do
  let initialWalProgress = WalProgress 0 0 0
  walProgressState <- WalProgressState <$> newMVar initialWalProgress
  result <- exec conn $ startReplicationCommand slotName systemLogPos
  case result of
    Nothing -> putStrLn "Woopsie" >>= \_ -> pure ()
    Just r  -> do
      status <- resultStatus r
      case status of
        CopyBoth -> do
          keepAliveChan <- newChan
          kTid <- uninterruptibleMask_ $ forkIO $ keepAliveHandler conn keepAliveChan walProgressState `finally` cleanupKeepAliveHandler
          hTid <- uninterruptibleMask_ $ forkIO $ handleCopyOutData keepAliveChan walProgressState conn `finally` cleanupHandleCopyOutData
          killThread kTid
          killThread hTid
        _ -> do
          err <- errorMessage conn
          print err

keepAliveHandler :: Connection -> Chan PrimaryKeepAlive -> WalProgressState -> IO ()
keepAliveHandler conn msgs (WalProgressState walState) = forever $ do
  throwIO $ ReplicantException "SLKDFJLSKDJFLSKDJF"
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
        CopyInWouldBlock -> putStrLn "Copy In Would Block!"

cleanupKeepAliveHandler :: IO ()
cleanupKeepAliveHandler = do
  putStrLn "I should clean up resources and rethrow?"
  throwIO (ReplicantException "keep alive thread failed")

cleanupHandleCopyOutData :: IO ()
cleanupHandleCopyOutData = do
  putStrLn "cleanupHandleCopyOutData should clean up resources and rethrow?"
  throwIO (ReplicantException "copy out data handler failed")
