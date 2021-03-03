module Database.PostgreSQL.Replicant.Message where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.Serialize
import Data.Text (Text)
import Data.Word
import GHC.Generics
import GHC.Int

newtype Oid a = Oid Int
  deriving (Eq, Show)

data Begin
  = Begin
  { _beginLsn        :: Int
  , _beginCommitTime :: Integer
  , _beginRemoteXid  :: Int
  }
  deriving (Eq, Show)

data Commit
  = Commit
    { _commitLsn    :: Int
    , _commitEndLsn :: Int
    , _commitTime   :: Integer
    }
    deriving (Eq, Show)

data Column = Column deriving (Eq, Show)

data Origin
  = Origin
  { _originCommitLsn :: Int
  , _originName      :: Text
  }
  deriving (Eq, Show)

data Relation
  = Relation
  { _relationId              :: Oid Relation
  , _relationNamespace       :: Text
  , _relationName            :: Text
  , _relationReplicaIdentity :: Int
  , _relationColumns         :: [Column]
  }
  deriving (Eq, Show)

data Insert
  = Insert
  { _insertRelationId :: Oid Relation
  , _insertData       :: [Tuple]
  }
  deriving (Eq, Show)

data Tuple = Tuple
  deriving (Eq, Show)

data ReplicationMessage
  = BeginMessage Begin
  | CommitMessage Commit
  | OriginMessage Origin
  | Relationmessage Relation
  | InsertMessage Insert
  | Unsupported
  deriving (Eq, Show)

data ResponseExpectation
  = ShouldRespond
  | DoNotRespond
  deriving (Eq, Generic, Show)

data ParseError
  = InvalidResponseExpectation
  deriving (Eq, Show)

responseExpectation :: Word8 -> Either ParseError ResponseExpectation
responseExpectation 0 = Right DoNotRespond
responseExpectation 1 = Right ShouldRespond
responseExpectation _ = Left InvalidResponseExpectation

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
    case responseExpectation of
      ShouldRespond -> putWord8 1
      DoNotRespond  -> putWord8 0

  get = do
    _ <- getBytes 1
    walEnd <- getInt64be
    sendTime <- getInt64be
    eitherFlag <- responseExpectation <$> getWord8
    case eitherFlag of
      Left err   -> fail "Could not decode response flag"
      Right flag -> pure $ PrimaryKeepAlive walEnd sendTime flag

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
    case responseExpectation of
      ShouldRespond -> putWord8 1
      DoNotRespond  -> putWord8 0

  get = do
    _ <- getBytes 1 -- should expect 0x72, 'r'
    walReceived <- getInt64be
    walFlushed  <- getInt64be
    walApplied  <- getInt64be
    sendTime    <- getInt64be
    eitherFlag  <- responseExpectation <$> getWord8
    case eitherFlag of
      Left err -> fail "Could not decode response flag"
      Right flag -> pure
        $ StandbyStatusUpdate walReceived walFlushed walApplied sendTime flag

data XLogData
  = XLogData
  { xLogDataWalStart :: Int64
  , xLogDataWalEnd :: Int64
  , xLogDataSendTime :: Int64
  , xLogdataWalData :: ByteString
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

consumeByteStringToEnd :: Get ByteString
consumeByteStringToEnd = do
  numRemaining <- remaining
  bs           <- getByteString numRemaining
  pure $ bs
