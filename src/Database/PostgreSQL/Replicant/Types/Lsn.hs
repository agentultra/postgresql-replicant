module Database.PostgreSQL.Replicant.Types.Lsn where

import Data.Attoparsec.ByteString.Char8
import Data.Bits
import Data.Bits.Extras
import Data.ByteString (ByteString ())
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Builder as Builder
import Data.ByteString.Lazy.Builder.ASCII (word32Hex, word32HexFixed)
import Data.Serialize
import Data.Word
import GHC.Int

data LSN = LSN
  { filepart :: !Int32 -- ^ Filepart
  , offset :: !Int32 -- ^ Offset
  }
  deriving (Show, Eq)

instance Ord LSN where
  compare (LSN l0 r0) (LSN l1 r1)
    | l0 > l1 || (l0 == l1 && r0 > r1) = GT
    | l0 < l1 || (l0 == l1 && r0 < r1) = LT
    | otherwise = EQ

instance Serialize LSN where
  put lsn = do
    putInt64be (toInt64 lsn)

  get = do
    i <- getInt64be
    pure $ fromInt64 i

-- | Convert an LSN to a 64-bit integer
toInt64 :: LSN -> Int64
toInt64 (LSN lo hi) =
  let r = w64 lo `shiftL` 32
  in fromIntegral $ r .|. fromIntegral hi

-- | Convert a 64-bit integer to an LSN
fromInt64 :: Int64 -> LSN
fromInt64 x =
  let mask = w64 $ maxBound @Word32
      hi = fromIntegral . w32 $ mask .&. fromIntegral x
      lo = fromIntegral $ x `shiftR` 32
  in LSN lo hi

lsnParser :: Parser LSN
lsnParser = LSN <$> (hexadecimal <* char '/') <*> hexadecimal

fromByteString :: ByteString -> Either String LSN
fromByteString = parseOnly lsnParser

toByteString :: LSN -> ByteString
toByteString (LSN filepart off) = BL.toStrict $ Builder.toLazyByteString (word32Hex (fromIntegral filepart) <> Builder.char7 '/' <> word32Hex (fromIntegral off))

-- | Add a number of bytes from an LSN
add :: LSN -> Int64 -> LSN
add lsn bytes = fromInt64 . (+ bytes) . toInt64 $ lsn

-- | Subtract a number of bytes from an LSN
sub :: LSN -> Int64 -> LSN
sub lsn bytes = fromInt64 . flip (-) bytes . toInt64 $ lsn

-- | Subtract two LSN's to calculate the difference of bytes between
-- them.
subLsn :: LSN -> LSN -> Int64
subLsn lsn1 lsn2 = toInt64 lsn1 - toInt64 lsn2
