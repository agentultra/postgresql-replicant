{-|
Module      : Database.PostgreSQL.Replicant.Types.Lsn
Description : Types and parsers for LSNs
Copyright   : (c) James King, 2020, 2021
License     : BSD3
Maintainer  : james@agentultra.com
Stability   : experimental
Portability : POSIX

/Log Sequence Number/ or LSN is a pointer to a place inside of a WAL
log file.  It contains the file name and an offset in bytes encoded in
two parts.

LSNs can be serialized into 64-bit big-endian numbers in the binary
protocol but are also represented textually in query results and other
places.

This module follows a similar convention to many containers libraries
and should probably be imported qualified to avoid name clashes if
needed.

See: https://www.postgresql.org/docs/10/datatype-pg-lsn.html
-}
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
toInt64 (LSN filePart offSet) =
  let r = w64 filePart `shiftL` 32
  in fromIntegral $ r .|. fromIntegral offSet

-- | Convert a 64-bit integer to an LSN
fromInt64 :: Int64 -> LSN
fromInt64 x =
  let mask = w64 $ maxBound @Word32
      offSet = fromIntegral . w32 $ mask .&. fromIntegral x
      filePart = fromIntegral $ x `shiftR` 32
  in LSN filePart offSet

lsnParser :: Parser LSN
lsnParser = LSN <$> (hexadecimal <* char '/') <*> hexadecimal

fromByteString :: ByteString -> Either String LSN
fromByteString = parseOnly lsnParser

-- | Note that as of bytestring ~0.10.12.0 we don't have upper-case
-- hex encoders but the patch to add them has been merged and when
-- available we should switch to them
toByteString :: LSN -> ByteString
toByteString (LSN filepart off) = BL.toStrict
  $ Builder.toLazyByteString
  ( word32Hex (fromIntegral filepart)
    <> Builder.char7 '/'
    <> word32Hex (fromIntegral off)
  )

-- | Add a number of bytes to an LSN
add :: LSN -> Int64 -> LSN
add lsn bytes = fromInt64 . (+ bytes) . toInt64 $ lsn

-- | Subtract a number of bytes from an LSN
sub :: LSN -> Int64 -> LSN
sub lsn bytes = fromInt64 . flip (-) bytes . toInt64 $ lsn

-- | Subtract two LSN's to calculate the difference of bytes between
-- them.
subLsn :: LSN -> LSN -> Int64
subLsn lsn1 lsn2 = toInt64 lsn1 - toInt64 lsn2
