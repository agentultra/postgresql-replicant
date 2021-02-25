import Test.Hspec

import Data.Serialize
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import GHC.Int

import Database.PostgreSQL.Replicant.Message

examplePrimaryKeepAliveMessage :: ByteString
examplePrimaryKeepAliveMessage
  = B.concat [(B.pack [0x6B]), (encode @Int64 123), (encode @Int64 346), (encode ShouldRespond)]

main :: IO ()
main = hspec $ do
  context "Message" $ do
    describe "ResponseExpectation" $ do
      it "should decode a ShouldRespond" $ do
        (decode $ B.pack [0x1])
          `shouldBe`
          (Right ShouldRespond)
      it "should decode DoNotRespond" $ do
        (decode $ B.pack [0x0])
          `shouldBe`
          (Right DoNotRespond)
    describe "PrimaryKeepAlive" $ do
      it "should decode a valid primary keep alive message" $ do
        (decode $ examplePrimaryKeepAliveMessage)
          `shouldBe`
          (Right $ PrimaryKeepAlive 123 346 ShouldRespond)
