module Database.PostgreSQL.Replicant.Types.Lsn where

import Data.Bits
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
  put (LSN filePart offSet) = do
    putInt32be filePart
    putWord8 0x2f -- '/' in ascii
    putInt32be offSet

  get = do
    filePart <- getInt32be
    skip 1
    LSN filePart <$> getInt32be

-- | Convert an LSN to a 64-bit integer
toInt64 :: LSN -> Int64
toInt64 (LSN lo hi) = undefined

-- | Convert a 64-bit integer to an LSN
int64ToLSN :: Int64 -> LSN
int64ToLSN = undefined

-- | Add a number of bytes from an LSN
add :: Int64 -> LSN -> LSN
add bytes = int64ToLSN . (+ bytes) . toInt64

-- | Subtract a number of bytes from an LSN
sub :: Int64 -> LSN -> LSN
sub bytes = int64ToLSN . flip (-) bytes . toInt64

-- | Subtract two LSN's to calculate the difference of bytes between
-- them.
subLsn :: LSN -> LSN -> Int64
subLsn lsn1 lsn2 = toInt64 lsn1 - toInt64 lsn2
