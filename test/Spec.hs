import Test.Hspec

import Data.Serialize
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import Data.Word
import GHC.Int

import Database.PostgreSQL.Replicant.Message

examplePrimaryKeepAliveMessage :: ByteString
examplePrimaryKeepAliveMessage
  = B.concat
    [ (B.pack [0x6B])
    , (encode @Int64 123)
    , (encode @Int64 346)
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
        (encode $ StandbyStatusUpdate 213 232 234 454 DoNotRespond)
          `shouldBe`
          B.concat
          [ B.pack [0x72]
          , encode @Int64 213
          , encode @Int64 232
          , encode @Int64 234
          , encode @Int64 454
          , encode @Word8 0
          ]

    describe "XLogData" $ do
      it "should encode/decode a valid xLogData message" $ do
        let msg = XLogData 123 234 345 (B8.pack "hello")
        (decode . encode $ msg)
          `shouldBe`
          Right msg

    describe "HotStandbyFeedback" $ do
      it "should encode/decode a valid HotStandbyFeedback message" $ do
        let msg = HotStandbyFeedback 123 234 456
        (decode . encode $ msg)
          `shouldBe`
          Right msg
