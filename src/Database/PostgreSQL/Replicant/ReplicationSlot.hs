{-|
Module      : Database.PostgreSQL.Replicant.ReplicationSlot
Description : Replication slot query commands
Copyright   : (c) James King, 2020, 2021
License     : BSD3
Maintainer  : james@agentultra.com
Stability   : experimental
Portability : POSIX

This module contains the PostgreSQL queries, types, and functions for
working with querying, creating, and working with replication slots.
-}
module Database.PostgreSQL.Replicant.ReplicationSlot where

import Control.Exception
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import Database.PostgreSQL.LibPQ

import Database.PostgreSQL.Replicant.Exception
import Database.PostgreSQL.Replicant.PostgresUtils
import Database.PostgreSQL.Replicant.Types.Lsn

data ReplicationSlotInfo
  = ReplicationSlotInfo
  { slotName    :: ByteString
  , slotPlugin  :: ByteString
  , slotType    :: ReplicationSlotType
  , slotActive  :: ReplicationSlotActive
  , slotRestart :: LSN
  }
  deriving (Eq, Show)

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

createReplicationSlotCommand :: Connection -> ByteString -> IO ByteString
createReplicationSlotCommand conn slotName = do
  escapedName <- escapeIdentifier conn slotName
  case escapedName of
    Nothing -> throwIO $ ReplicantException $ "Invalid slot name: " ++ show slotName
    Just escaped ->
      pure $
      B.intercalate
      ""
      [ "CREATE_REPLICATION_SLOT"
      , escaped
      , "LOGICAL wal2json"
      ]

-- | Create a replication slot using synchronous query execution.
--
-- May throw an exception if the command fails.
createReplicationSlotSync :: Connection -> ByteString -> IO ReplicationSlotInfo
createReplicationSlotSync conn slotName = do
  createReplicationSlotQuery <- createReplicationSlotCommand conn slotName
  result <- exec conn createReplicationSlotQuery
  case result of
    Just r -> do
      resultStatus <- resultStatus r
      case resultStatus of
        TuplesOk -> do
          sName           <- getvalue' r (toRow 0) (toColumn 0)
          consistentPoint <- getvalue' r (toRow 0) (toColumn 1)
          outputPlugin    <- getvalue' r (toRow 0) (toColumn 3)
          case (sName, consistentPoint, outputPlugin) of
            (Just s, Just c, Just op) ->
              case fromByteString c of
                Left _ -> throwIO $ ReplicantException "createReplicationSlotSync: invalid LSN detected"
                Right lsn -> pure $ ReplicationSlotInfo s op Logical Active lsn
            _ -> do
              err <- maybe "createReplicationSlotSync: unknown error" id <$> errorMessage conn
              throwIO $ ReplicantException (B8.unpack err)
        _ -> do
          err <- maybe "createReplicationSlotSync: unknown error" id <$> errorMessage conn
          throwIO $ ReplicantException (B8.unpack err)
    _ -> do
      err <- maybe "createReplicationSlotSync: unknown error" id <$> errorMessage conn
      throwIO $ ReplicantException (B8.unpack err)

getReplicationSlotInfoCommand :: Connection -> ByteString -> IO ByteString
getReplicationSlotInfoCommand conn slotName = do
  escapedName <- escapeStringConn conn slotName
  case escapedName of
    Nothing -> throwIO $ ReplicantException $ "Invalid slot name: " ++ show slotName
    Just escaped ->
      pure $
      B.intercalate
      ""
      [ "select slot_name, plugin, slot_type, active, restart_lsn from pg_replication_slots where slot_name = '"
      , escaped
      , "';"
      ]

-- | Get information about an existing replication slot.  Returns
-- @Nothing@ when the requested slot cannot be found.
--
-- May throw an exception if the command query fails.
getReplicationSlotSync :: Connection -> ByteString -> IO (Maybe ReplicationSlotInfo)
getReplicationSlotSync conn slotName = do
  replicationSlotInfoQuery <- getReplicationSlotInfoCommand conn slotName
  result <- exec conn replicationSlotInfoQuery
  case result of
    Just r -> do
      resultStatus <- resultStatus r
      case resultStatus of
        TuplesOk -> do
          nRows <- ntuples r
          if nRows == 0
            then pure Nothing
            else do
            slotName    <- getvalue' r (toRow 0) (toColumn 0)
            slotPlugin  <- getvalue' r (toRow 0) (toColumn 1)
            slotType    <- getvalue' r (toRow 0) (toColumn 2)
            slotActive  <- getvalue' r (toRow 0) (toColumn 3)
            slotRestart <- getvalue' r (toRow 0) (toColumn 4)
            case (slotName, slotPlugin, slotType, slotActive, slotRestart) of
              (Just n, Just p, Just t, Just a, Just restart) -> do
                case fromByteString restart of
                  Left _ -> pure Nothing -- TODO: this shouldn't happen...
                  Right lsn -> pure $ Just $ ReplicationSlotInfo n p (parseSlotType t) (parseSlotActive a) lsn
              _ ->  pure Nothing
        _ -> pure Nothing
    _ -> do
      err <- maybe "getReplicationSlotSync: unknown error" id <$> errorMessage conn
      throwIO $ ReplicantException (B8.unpack err)

-- | Create replication slot or retrieve an existing slot.
--
-- Can throw exceptions from @getReplicationSlotSync@ or
-- @createReplicationSlotSync@.
setupReplicationSlot :: Connection -> ByteString -> IO ReplicationSlotInfo
setupReplicationSlot conn slotName = do
  maybeSlot <- getReplicationSlotSync conn slotName
  case maybeSlot of
    Just slot -> pure $ slot
    Nothing   -> createReplicationSlotSync conn slotName
