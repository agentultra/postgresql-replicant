module Database.PostgreSQL.Replicant.PostgresUtils where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.Fixed
import Data.Time
import Database.PostgreSQL.LibPQ
import GHC.Int

import Database.PostgreSQL.Replicant.Types.Lsn

postgresEpoch :: IO Int64
postgresEpoch = do
  let epoch = mkUTCTime (2000, 1, 1) (0, 0, 0)
  now <- getCurrentTime
  pure $ round . (* 1000000) . nominalDiffTimeToSeconds $ diffUTCTime now epoch

-- From https://www.williamyaoh.com/posts/2019-09-16-time-cheatsheet.html
mkUTCTime :: (Integer, Int, Int)
          -> (Int, Int, Pico)
          -> UTCTime
mkUTCTime (year, mon, day) (hours, mins, secs) =
  UTCTime (fromGregorian year mon day)
          (timeOfDayToTime (TimeOfDay hours mins secs))

data ReplicationSlotType = Logical | Physical | UnknownSlotType
  deriving (Eq, Show)

parseSlotType :: ByteString -> ReplicationSlotType
parseSlotType "logical"  = Logical
parseSlotType "physical" = Physical
parseSlotType _          = UnknownSlotType

data ReplicationSlotActive = Active | Inactive
  deriving (Eq, Show)

parseSlotActive :: ByteString -> ReplicationSlotActive
parseSlotActive "t" = Active
parseSlotActive "f" = Inactive
parseSlotActive _   = Inactive

data ReplicationSlotInfo
  = ReplicationSlotInfo
  { slotName    :: ByteString
  , slotPlugin  :: ByteString
  , slotType    :: ReplicationSlotType
  , slotActive  :: ReplicationSlotActive
  , slotRestart :: LSN
  }
  deriving (Eq, Show)

getReplicationSlotInfoCommand :: Connection -> ByteString -> IO (Maybe ByteString)
getReplicationSlotInfoCommand conn slotName = do
  escapedName <- escapeStringConn conn slotName
  case escapedName of
    Nothing -> pure Nothing
    Just escaped ->
      pure $ Just $
      B.intercalate
      ""
      [ "select slot_name, plugin, slot_type, active, restart_lsn from pg_replication_slots where slot_name = '"
      , escaped
      , "';"
      ]

getReplicationSlotSync :: Connection -> ByteString -> IO (Maybe ReplicationSlotInfo)
getReplicationSlotSync conn slotName = do
  replicationSlotInfoQuery <- getReplicationSlotInfoCommand conn slotName
  case replicationSlotInfoQuery of
    Nothing -> pure Nothing
    Just query -> do
      result <- exec conn query
      case result of
        Nothing -> pure Nothing
        Just r  -> do
          slotName    <- getvalue' r (toRow 0) (toColumn 0)
          slotPlugin  <- getvalue' r (toRow 0) (toColumn 1)
          slotType    <- getvalue' r (toRow 0) (toColumn 2)
          slotActive  <- getvalue' r (toRow 0) (toColumn 3)
          slotRestart <- getvalue' r (toRow 0) (toColumn 4)
          case (slotName, slotPlugin, slotType, slotActive, slotRestart) of
            (Just n, Just p, Just t, Just a, Just r) -> do
              case fromByteString r of
                Left _ -> pure Nothing -- TODO: this shouldn't happen...
                Right lsn -> pure $ Just $ ReplicationSlotInfo n p (parseSlotType t) (parseSlotActive a) lsn
