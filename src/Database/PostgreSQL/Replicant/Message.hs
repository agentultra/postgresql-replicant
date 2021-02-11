module Database.PostgreSQL.Replicant.Message where

import Data.Text (Text)

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
