module Database.PostgreSQL.Replicant.Types.Lsn where

import Data.Serialize
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

toInt64 :: LSN -> Int64
toInt64 = undefined

int64ToLSN :: Int64 -> LSN
int64ToLSN = undefined

add :: Int64 -> LSN -> LSN
add bytes = int64ToLSN . (+ bytes) . toInt64

sub :: Int64 -> LSN -> LSN
sub bytes = int64ToLSN . flip (-) bytes . toInt64
