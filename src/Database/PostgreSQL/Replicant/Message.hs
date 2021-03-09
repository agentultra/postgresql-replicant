module Database.PostgreSQL.Replicant.Message where

import Control.Applicative
import Control.Monad
import Data.Aeson
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.Scientific (Scientific)
import Data.Serialize
import Data.Text (Text)
import Data.Word
import GHC.Generics
import GHC.Int

import Database.PostgreSQL.Replicant.Serialize

-- WAL Replication Stream messages

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

data PrimaryKeepAlive
  = PrimaryKeepAlive
  { primaryKeepAliveWalEnd              :: Int64
  , primaryKeepAliveSendTime            :: Int64
  , primaryKeepAliveResponseExpectation :: ResponseExpectation
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

data StandbyStatusUpdate
  = StandbyStatusUpdate
  { standbyStatuUpdateLastWalByteReceived  :: Int64
  , standbyStatusUpdateLastWalByteFlushed  :: Int64
  , standbyStatusUpdateLastWalByteApplied  :: Int64
  , standbyStatusUpdateSendTime            :: Int64
  , standbyStatusUpdateResponseExpectation :: ResponseExpectation
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
    putInt64be walReceived
    putInt64be walFlushed
    putInt64be walApplied
    putInt64be sendTime
    put responseExpectation

  get = do
    _ <- getBytes 1 -- should expect 0x72, 'r'
    walReceived         <- getInt64be
    walFlushed          <- getInt64be
    walApplied          <- getInt64be
    sendTime            <- getInt64be
    responseExpectation <- get
    pure $
      StandbyStatusUpdate
      walReceived
      walFlushed
      walApplied
      sendTime
      responseExpectation

data XLogData
  = XLogData
  { xLogDataWalStart :: Int64
  , xLogDataWalEnd   :: Int64
  , xLogDataSendTime :: Int64
  , xLogDataWalData  :: ByteString
  }
  deriving (Eq, Generic, Show)

instance Serialize XLogData where
  put (XLogData walStart walEnd sendTime walData) = do
    putWord8 0x77 -- 'w'
    putInt64be walStart
    putInt64be walEnd
    putInt64be sendTime
    putByteString walData

  get = do
    _ <- getBytes 1 -- should expec '0x77', 'w'
    walStart <- getInt64be
    walEnd   <- getInt64be
    sendTime <- getInt64be
    walData  <- consumeByteStringToEnd
    pure $ XLogData walStart walEnd sendTime walData

data HotStandbyFeedback
  = HotStandbyFeedback
  { hotStandbyFeedbackClientSendTime :: Int64
  , hotStandbyFeedbackCurrentXmin    :: Int32
  , hotStandbyFeedbackCurrentEpoch   :: Int32
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

data WalCopyData
  = XLogDataM XLogData
  | KeepAliveM PrimaryKeepAlive
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

data Column
  = Column
  { columnName  :: Text
  , columnType  :: Text
  , columnValue :: WalValue
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

data Insert
  = Insert
  { insertSchema  :: String
  , insertTable   :: String
  , insertColumns :: [Column]
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

data Update
  = Update
  { updateSchema  :: Text
  , updateTable   :: Text
  , updateColumns :: [Column]
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

data Delete
  = Delete
  { deleteSchema  :: Text
  , deleteTable   :: Text
  , deleteColumns :: [Column]
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

data Message
  = Message
  { messageTransactional :: Bool
  , messagePrefix        :: Text
  , messageContent       :: Text
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
  = WInsert Insert
  | WUpdate Update
  | WDelete Delete
  | WMessage Message
  deriving (Eq, Generic, Show)

instance ToJSON WalLogData where
  toJSON = genericToJSON defaultOptions { sumEncoding = UntaggedValue }

instance FromJSON WalLogData where
  parseJSON = genericParseJSON defaultOptions { sumEncoding = UntaggedValue }

newtype Change = Change [WalLogData]
  deriving (Eq, Generic, Show)

instance ToJSON Change where
  toJSON (Change walLogData) = object [ "change" .= walLogData ]

instance FromJSON Change where
  parseJSON = withObject "Change" $ \o -> Change <$> o .: "change"
