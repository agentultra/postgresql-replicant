module Database.PostgreSQL.Replicant.PostgresUtils where

import Data.Fixed
import Data.Serialize
import Data.Time
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


postgresEpoch :: IO Int64
postgresEpoch = do
  let epoch = mkUTCTime (2000, 1, 1) (0, 0, 0)
  now <- getCurrentTime
  pure $ round . (* 1000000) . nominalDiffTimeToSeconds $ diffUTCTime now epoch

-- Utilities

-- From https://www.williamyaoh.com/posts/2019-09-16-time-cheatsheet.html
mkUTCTime :: (Integer, Int, Int)
          -> (Int, Int, Pico)
          -> UTCTime
mkUTCTime (year, mon, day) (hours, mins, secs) =
  UTCTime (fromGregorian year mon day)
          (timeOfDayToTime (TimeOfDay hours mins secs))
