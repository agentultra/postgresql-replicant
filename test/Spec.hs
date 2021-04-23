import Test.Hspec

import Data.Serialize
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import Data.Word
import GHC.Int

import Database.PostgreSQL.Replicant.Message
import Database.PostgreSQL.Replicant.Types.Lsn
import qualified Database.PostgreSQL.Replicant.Queue as Q

examplePrimaryKeepAliveMessage :: ByteString
examplePrimaryKeepAliveMessage
  = B.concat
    [ (B.pack [0x6B])
    , (encode @LSN (fromInt64 123))
    , (encode @LSN (fromInt64 346))
    , (encode @Word8 1)
    ]

main :: IO ()
main = hspec $ do
  context "Message" $ do
    describe "PrimaryKeepAlive" $ do
      it "should decode a valid primary keep alive message" $ do
        (decode $ examplePrimaryKeepAliveMessage)
          `shouldBe`
          (Right $ PrimaryKeepAlive 123 346 ShouldRespond)

    describe "StandbyStatusUpdate" $ do
      it "should encode a valid standby status update message" $ do
        (encode $ StandbyStatusUpdate
         (fromInt64 213)
         (fromInt64 232)
         (fromInt64 234)
         454
         DoNotRespond)
          `shouldBe`
          B.concat
          [ B.pack [0x72]
          , encode @LSN (fromInt64 213)
          , encode @LSN (fromInt64 232)
          , encode @LSN (fromInt64 234)
          , encode @Int64 454
          , encode @Word8 0
          ]

    describe "XLogData" $ do
      it "should encode/decode a valid xLogData message" $ do
        let msg = XLogData (fromInt64 123) (fromInt64 234) 345 (B8.pack "hello")
        (decode . encode $ msg)
          `shouldBe`
          Right msg

    describe "HotStandbyFeedback" $ do
      it "should encode/decode a valid HotStandbyFeedback message" $ do
        let msg = HotStandbyFeedback 123 234 456
        (decode . encode $ msg)
          `shouldBe`
          Right msg

    describe "WalCopyData" $ do
      it "should encode/decode an XLogData message" $ do
        let msg = XLogDataM (XLogData (fromInt64 123) (fromInt64 234) 345 (B8.pack "hello"))
        (decode . encode $ msg)
          `shouldBe`
          Right msg

      it "should encode/decode a PrimaryKeepAlive message" $ do
        let msg = KeepAliveM (PrimaryKeepAlive 123 346 ShouldRespond)
        (decode . encode $ msg)
          `shouldBe`
          Right msg

  context "Types" $ do
    describe "LSN" $ do
      context "Serializable" $ do
        it "should be serializable" $ do
          let lsn = LSN 2 23
          (decode . encode $ lsn) `shouldBe` Right lsn

        it "should be equivalent to fromByteString/toByteString" $ do
          let (Right lsn) = fromByteString "16/3002D50"
          (toByteString <$> (decode . encode @LSN $ lsn)) `shouldBe` Right "16/3002d50"

  describe "FifoQueue" $ do
    it "should dequeue Nothing if the queue is empty" $ do
      queue <- Q.empty @Int
      res <- Q.dequeue queue
      res `shouldBe` Nothing

    it "should respect first-in, first-out" $ do
      queue <- Q.empty
      Q.enqueue queue 1
      Q.enqueue queue 2
      Q.enqueue queue 3
      res <- Q.dequeue queue
      res `shouldBe` Just 1

    context "null" $ do
      it "should return True if the queue is empty" $ do
        queue <- Q.empty
        res <- Q.null queue
        res `shouldBe` True

      it "should return False if the queue has at least one element" $ do
        queue <- Q.empty
        Q.enqueue queue 1
        res <- Q.null queue
        res `shouldBe` False

    context "enqueueRight" $ do
      it "should enqueue an item at the head of the queue" $ do
        queue <- Q.empty
        Q.enqueue queue 1
        Q.enqueueRight queue 2
        res <- Q.dequeue queue
        res `shouldBe` Just 2
