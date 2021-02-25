module Database.PostgreSQL.Replicant.Message where

import Data.Serialize
import Data.Text (Text)
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

instance Serialize ResponseExpectation where
  put ShouldRespond = putWord8 1
  put DoNotRespond = putWord8 0

  get = do
    flag <- getWord8
    case flag of
      0 -> pure DoNotRespond
      1 -> pure ShouldRespond
      _ -> fail "Invalid response expectation flag"

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
    flagByte <- getByteString 1
    case decode @ResponseExpectation flagByte of
      Left err -> fail "Could not decode response flag"
      Right responseExpectation ->
        pure $ PrimaryKeepAlive walEnd sendTime responseExpectation
