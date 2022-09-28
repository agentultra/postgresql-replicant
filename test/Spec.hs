import Test.Hspec

import Data.Binary
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import Data.Word
import GHC.Int

import Database.PostgreSQL.Replicant.Message
import Database.PostgreSQL.Replicant.Settings
import Database.PostgreSQL.Replicant.Types.Lsn

examplePrimaryKeepAliveMessage :: BL.ByteString
examplePrimaryKeepAliveMessage
  = BL.concat
    [ (BL.pack [0x6B])
    , (encode @LSN (fromInt64 123))
    , (encode @LSN (fromInt64 346))
    , (encode @Word8 1)
    ]

main :: IO ()
main = hspec $ do
  context "Message" $ do
    describe "PrimaryKeepAlive" $ do
      it "should decode a valid primary keep alive message" $ do
        let Right (_, _, result)
              = decodeOrFail
              $ examplePrimaryKeepAliveMessage
        result `shouldBe` (PrimaryKeepAlive 123 346 ShouldRespond)

    describe "StandbyStatusUpdate" $ do
      it "should encode a valid standby status update message" $ do
        let expected
              = BL.concat
              [ BL.pack [0x72]
              , encode @LSN (fromInt64 213)
              , encode @LSN (fromInt64 232)
              , encode @LSN (fromInt64 234)
              , encode @Int64 454
              , encode @Word8 0
              ]
        (encode $ StandbyStatusUpdate
         (fromInt64 213)
         (fromInt64 232)
         (fromInt64 234)
         454
         DoNotRespond)
          `shouldBe`
          expected

    describe "XLogData" $ do
      it "should encode/decode a valid xLogData message" $ do
        let msg = XLogData (fromInt64 123) (fromInt64 234) 345 (B8.pack "hello")
        (decode @XLogData . encode $ msg) `shouldBe` msg

    describe "HotStandbyFeedback" $ do
      it "should encode/decode a valid HotStandbyFeedback message" $ do
        let msg = HotStandbyFeedback 123 234 456
        (decode @HotStandbyFeedback . encode $ msg) `shouldBe` msg

    describe "WalCopyData" $ do
      it "should encode/decode an XLogData message" $ do
        let msg = XLogDataM (XLogData (fromInt64 123) (fromInt64 234) 345 (B8.pack "hello"))
        (decode @WalCopyData . encode $ msg) `shouldBe` msg

      it "should encode/decode a PrimaryKeepAlive message" $ do
        let msg = KeepAliveM (PrimaryKeepAlive 123 346 ShouldRespond)
        (decode @WalCopyData . encode $ msg) `shouldBe` msg

  context "Types" $ do
    describe "LSN" $ do
      context "Binary" $ do
        it "should be serializable" $ do
          let lsn = LSN 2 23
          (decode @LSN . encode $ lsn) `shouldBe` lsn

        it "should be equivalent to fromByteString/toByteString" $ do
          let (Right lsn) = fromByteString "16/3002D50"
          (toByteString $ (decode @LSN . encode @LSN $ lsn)) `shouldBe` "16/3002d50"

  context "Settings" $ do
    describe "pgConnectionString" $ do
      it "should include the password when specified" $ do
        let settings
              = PgSettings
              { pgUser = "foo"
              , pgPassword = Just "bar"
              , pgDbName = "test"
              , pgHost = "hostname"
              , pgPort = "5432"
              , pgSlotName = "test-slot"
              , pgUpdateDelay = "43"
              }
        pgConnectionString settings `shouldBe` "user=foo pass=bar dbname=test host=hostname port=5432 replication=database"

      it "should omit the password when not specified" $ do
        let settings
              = PgSettings
              { pgUser = "foo"
              , pgPassword = Nothing
              , pgDbName = "test"
              , pgHost = "hostname"
              , pgPort = "5432"
              , pgSlotName = "test-slot"
              , pgUpdateDelay = "43"
              }
        pgConnectionString settings `shouldBe` "user=foo  dbname=test host=hostname port=5432 replication=database"
