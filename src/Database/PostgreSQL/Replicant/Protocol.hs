module Database.PostgreSQL.Replicant.Protocol where

import Control.Monad (forever)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import Database.PostgreSQL.LibPQ

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

startReplicationStream :: Connection -> ByteString -> ByteString -> IO ()
startReplicationStream conn slotName systemLogPos = do
  result <- exec conn $ startReplicationCommand slotName systemLogPos
  case result of
    Nothing -> putStrLn "Woopsie" >>= \_ -> pure ()
    Just r  -> do
      status <- resultStatus r
      case status of
        CopyBoth -> forever $ do
          d <- getCopyData conn False
          case d of
            CopyOutRow row -> do
              print row
            CopyOutError -> do
              err <- errorMessage conn
              print err
              pure ()
            CopyOutDone -> do
              putStrLn "We should be finished with copying?"
              pure ()
            _ -> do
              putStrLn "Unknown problem with getCopyData"
              pure ()
        _        -> do
          err <- errorMessage conn
          print err
          pure ()
