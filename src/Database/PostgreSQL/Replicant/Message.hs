{-# LANGUAGE Strict #-}

{-|
Module      : Database.PostgreSQL.Replicant.Message
Description : Streaming replication message types
Copyright   : (c) James King, 2020, 2021
License     : BSD3
Maintainer  : james@agentultra.com
Stability   : experimental
Portability : POSIX

This module contains the binary protocols messages used in the
streaming replication protocol as well as the messages used in the
body of the logical stream messages.
-}
module Database.PostgreSQL.Replicant.Message where

import Control.Monad
import Data.Aeson
import Data.ByteString (ByteString)
import Data.Scientific (Scientific)
import Data.Serialize
import Data.Text (Text)
import GHC.Generics
import GHC.Int

import Database.PostgreSQL.Replicant.Serialize
import Database.PostgreSQL.Replicant.Types.Lsn

-- WAL Replication Stream messages

-- | Indicates whether the server or the client should respond to the
-- message.
data ResponseExpectation
  = ShouldRespond
  | DoNotRespond
  deriving (Eq, Generic, Show)

instance Serialize ResponseExpectation where
  put ShouldRespond = putWord8 1
  put DoNotRespond  = putWord8 0

  get = do
    responseFlag <- getWord8
    case responseFlag of
      0 -> pure DoNotRespond
      1 -> pure ShouldRespond
      _ -> fail "Unrecognized response expectation flag"

-- | The Postgres WAL sender thread may periodically send these
-- messages.  When the server expects the client to respond it updates
-- its internal state of the client based on the response.  Failure to
-- respond results in the dreaded "WAL timeout" error.
--
-- See StandbyStatusUpdate for the message the client should respond
-- with.
data PrimaryKeepAlive
  = PrimaryKeepAlive
  { primaryKeepAliveWalEnd              :: !Int64
  , primaryKeepAliveSendTime            :: !Int64
  , primaryKeepAliveResponseExpectation :: !ResponseExpectation
  }
  deriving (Eq, Generic, Show)

instance Serialize PrimaryKeepAlive where
  put (PrimaryKeepAlive walEnd sendTime responseExpectation) = do
    putWord8 0x6B -- 'k'
    putInt64be walEnd
    putInt64be sendTime
    put responseExpectation

  get = do
    _ <- getBytes 1
    walEnd <- getInt64be
    sendTime <- getInt64be
    responseExpectation <- get
    pure $ PrimaryKeepAlive walEnd sendTime responseExpectation

-- | Sent by the client.  Can be sent periodically or in response to a
-- PrimaryKeepAlive message from the server.  Gives the server
-- information about the client stream state.
data StandbyStatusUpdate
  = StandbyStatusUpdate
  { standbyStatuUpdateLastWalByteReceived  :: !LSN
  , standbyStatusUpdateLastWalByteFlushed  :: !LSN
  , standbyStatusUpdateLastWalByteApplied  :: !LSN
  , standbyStatusUpdateSendTime            :: !Int64
  , standbyStatusUpdateResponseExpectation :: !ResponseExpectation
  }
  deriving (Eq, Generic, Show)

instance Serialize StandbyStatusUpdate where
  put (StandbyStatusUpdate
       walReceived
       walFlushed
       walApplied
       sendTime
       responseExpectation) = do
    putWord8 0x72 -- 'r'
    put walReceived
    put walFlushed
    put walApplied
    putInt64be sendTime
    put responseExpectation

  get = do
    _ <- getBytes 1 -- should expect 0x72, 'r'
    walReceived         <- get
    walFlushed          <- get
    walApplied          <- get
    sendTime            <- getInt64be
    responseExpectation <- get
    pure $
      StandbyStatusUpdate
      walReceived
      walFlushed
      walApplied
      sendTime
      responseExpectation

-- | Carries WAL segments in the streaming protocol.
data XLogData
  = XLogData
  { xLogDataWalStart :: !LSN
  , xLogDataWalEnd   :: !LSN
  , xLogDataSendTime :: !Int64
  , xLogDataWalData  :: !ByteString
  }
  deriving (Eq, Generic, Show)

instance Serialize XLogData where
  put (XLogData walStart walEnd sendTime walData) = do
    putWord8 0x77 -- 'w'
    put walStart
    put walEnd
    putInt64be sendTime
    putByteString walData

  get = do
    _ <- getBytes 1 -- should expec '0x77', 'w'
    walStart <- get
    walEnd   <- get
    sendTime <- getInt64be
    walData  <- consumeByteStringToEnd
    pure $ XLogData walStart walEnd sendTime walData

-- | Not used yet but enables streaming in hot-standby mode.
data HotStandbyFeedback
  = HotStandbyFeedback
  { hotStandbyFeedbackClientSendTime :: !Int64
  , hotStandbyFeedbackCurrentXmin    :: !Int32
  , hotStandbyFeedbackCurrentEpoch   :: !Int32
  }
  deriving (Eq, Generic, Show)

instance Serialize HotStandbyFeedback where
  put (HotStandbyFeedback clientSendTime currentXMin currentEpoch) = do
    putWord8 0x68
    putInt64be clientSendTime
    putInt32be currentXMin
    putInt32be currentEpoch

  get = do
    _ <- getBytes 1 -- should expect '0x68' 'h'
    clientSendTime <- getInt64be
    currentXmin <- getInt32be
    currentEpoch <- getInt32be
    pure $ HotStandbyFeedback clientSendTime currentXmin currentEpoch

-- | This structure wraps the two messages sent by the server so that
-- we get a Serialize instance for both.
data WalCopyData
  = XLogDataM !XLogData
  | KeepAliveM !PrimaryKeepAlive
  deriving (Eq, Generic, Show)

instance Serialize WalCopyData where
  put (XLogDataM xLogData)   = put xLogData
  put (KeepAliveM keepAlive) = put keepAlive
  get = do
    messageTypeFlag <- lookAhead $ getWord8
    case messageTypeFlag of
      -- 'w' XLogData
      0x77 -> do
        xLogData <- get
        pure $ XLogDataM xLogData
      -- 'k' PrimaryKeepAlive
      0x6B -> do
        keepAlive <- get
        pure $ KeepAliveM keepAlive
      _    -> fail "Unrecognized WalCopyData"

-- WAL Log Data

-- | Wraps Postgres values.  Since this library currently supports
-- only the @wal2json@ logical decoder plugin we have the JSON values
-- much like Aeson does.
data WalValue
  = WalString !Text
  | WalNumber !Scientific
  | WalBool !Bool
  | WalNull
  deriving (Eq, Generic, Show)

instance FromJSON WalValue where
  parseJSON (String txt) = pure $ WalString txt
  parseJSON (Number num) = pure $ WalNumber num
  parseJSON (Bool bool)  = pure $ WalBool bool
  parseJSON Null         = pure WalNull
  parseJSON _            = fail "Unrecognized WalValue"

instance ToJSON WalValue where
  toJSON (WalString txt) = String txt
  toJSON (WalNumber n)   = Number n
  toJSON (WalBool b)     = Bool b
  toJSON (WalNull)       = Null

-- | Represents a single table column.  We only support the `wal2json`
-- logical decoder plugin and make no attempt to parse anything but
-- JSON-like primitives.
data Column
  = Column
  { columnName  :: !Text
  , columnType  :: !Text
  , columnValue :: !WalValue
  }
  deriving (Eq, Show)

data ColumnParseError
  = ColumnLengthMatchError
  deriving (Eq, Show)

-- | Some WAL output plugins encode column values in three, equal
-- length, heterogeneous lists.
columns :: [Text] -> [Text] -> [WalValue] -> Either ColumnParseError [Column]
columns colNames colTypes colValues
  | length colNames == length colTypes && length colTypes == length colValues
  = Right (map toColumn $ zip3 colNames colTypes colValues)
  | otherwise = Left ColumnLengthMatchError
  where
    toColumn (n, t, v) = Column n t v

fromColumn :: Column -> (Text, Text, WalValue)
fromColumn (Column cName cType cValue) = (cName, cType, cValue)

fromColumns :: [Column] -> ([Text], [Text], [WalValue])
fromColumns = unzip3 . map fromColumn

-- | Represents a single insert query in the logical replication
-- format.
data Insert
  = Insert
  { insertSchema  :: !String
  , insertTable   :: !String
  , insertColumns :: ![Column]
  }
  deriving (Eq, Show)

instance FromJSON Insert where
  parseJSON = withObject "Insert" $ \o -> do
    kind         <- o .: "kind"
    unless (kind == String "insert") $ fail "Invalid insert"
    schema       <- o .: "schema"
    table        <- o .: "table"
    columnNames  <- o .: "columnnames"
    columnTypes  <- o .: "columntypes"
    columnValues <- o .: "columnvalues"
    case columns columnNames columnTypes columnValues of
      Left err      -> fail $ show err
      Right columns -> pure $ Insert schema table columns

instance ToJSON Insert where
  toJSON (Insert schema table columns) =
    let (cNames, cTypes, cValues) = fromColumns columns
    in object [ "kind"         .= String "insert"
              , "schema"       .= schema
              , "table"        .= table
              , "columnnames"  .= cNames
              , "columntypes"  .= cTypes
              , "columnvalues" .= cValues
              ]

-- | Represents a single update query in the logical replication
-- format.
data Update
  = Update
  { updateSchema  :: !Text
  , updateTable   :: !Text
  , updateColumns :: ![Column]
  }
  deriving (Eq, Show)

instance FromJSON Update where
  parseJSON = withObject "Insert" $ \o -> do
    kind         <- o .: "kind"
    unless (kind == String "update") $ fail "Invalid update"
    schema       <- o .: "schema"
    table        <- o .: "table"
    columnNames  <- o .: "columnnames"
    columnTypes  <- o .: "columntypes"
    columnValues <- o .: "columnvalues"
    case columns columnNames columnTypes columnValues of
      Left err      -> fail $ show err
      Right columns -> pure $ Update schema table columns

instance ToJSON Update where
  toJSON (Update schema table columns) =
    let (cNames, cTypes, cValues) = fromColumns columns
    in object [ "kind"         .= String "update"
              , "schema"       .= schema
              , "table"        .= table
              , "columnnames"  .= cNames
              , "columntypes"  .= cTypes
              , "columnvalues" .= cValues
              ]

-- | Represents a single delete query in the logical replication format
data Delete
  = Delete
  { deleteSchema  :: !Text
  , deleteTable   :: !Text
  , deleteColumns :: ![Column]
  }
  deriving (Eq, Show)

instance FromJSON Delete where
  parseJSON = withObject "Delete" $ \o -> do
    kind      <- o .: "kind"
    unless (kind == String "delete") $ fail "Invalid delete"
    schema    <- o .: "schema"
    table     <- o .: "table"
    oldKeys   <- o .: "oldkeys"
    keyNames  <- oldKeys .: "keynames"
    keyTypes  <- oldKeys .: "keytypes"
    keyValues <- oldKeys .: "keyvalues"
    case columns keyNames keyTypes keyValues of
      Left err      -> fail $ show err
      Right columns -> pure $ Delete schema table columns

instance ToJSON Delete where
  toJSON (Delete schema table columns) =
    let (cNames, cTypes, cValues) = fromColumns columns
    in object [ "kind"    .= String "delete"
              , "schema"  .= schema
              , "table"   .= table
              , "oldkeys" .= object
                [ "keynames"  .= cNames
                , "keytypes"  .= cTypes
                , "keyvalues" .= cValues
                ]
              ]

-- | Occasionally the server may also send these for informational
-- purposes and can be ignored.  May be used internally.
data Message
  = Message
  { messageTransactional :: !Bool
  , messagePrefix        :: !Text
  , messageContent       :: !Text
  }
  deriving (Eq, Show)

instance FromJSON Message where
  parseJSON = withObject "Message" $ \o -> do
    kind          <- o .: "kind"
    unless (kind == String "message") $ fail "Invalid message"
    transactional <- o .: "transactional"
    prefix        <- o .: "prefix"
    content       <- o .: "content"
    pure $ Message transactional prefix content

instance ToJSON Message where
  toJSON (Message transactional prefix content)
    = object
    [ "kind"          .= String "message"
    , "transactional" .= transactional
    , "prefix"        .= prefix
    , "content"       .= content
    ]

data WalLogData
  = WInsert !Insert
  | WUpdate !Update
  | WDelete !Delete
  | WMessage !Message
  deriving (Eq, Generic, Show)

instance ToJSON WalLogData where
  toJSON = genericToJSON defaultOptions { sumEncoding = UntaggedValue }

instance FromJSON WalLogData where
  parseJSON = genericParseJSON defaultOptions { sumEncoding = UntaggedValue }

data Change
  = Change
  { changeNextLSN :: LSN
    -- ^ Return this LSN in your callback to update the stream state
    -- in replicant
  , changeDeltas  :: [WalLogData]
    -- ^ The list of WAL log changes in this transaction.
  }
  deriving (Eq, Generic, Show)

instance ToJSON Change where
  toJSON = genericToJSON defaultOptions

instance FromJSON Change where
  parseJSON = withObject "Change" $ \o -> do
    nextLSN <- o .: "nextlsn"
    deltas  <- o .: "change"
    pure $ Change nextLSN deltas
