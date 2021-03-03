module Database.PostgreSQL.Replicant.Message where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.Serialize
import Data.Text (Text)
import Data.Word
import GHC.Generics
import GHC.Int

import Database.PostgreSQL.Replicant.Serialize

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
