module Database.PostgreSQL.Replicant.PostgresUtils where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.Fixed
import Data.Time
import Database.PostgreSQL.LibPQ
import GHC.Int

import Database.PostgreSQL.Replicant.Types.Lsn

postgresEpoch :: IO Int64
postgresEpoch = do
  let epoch = mkUTCTime (2000, 1, 1) (0, 0, 0)
  now <- getCurrentTime
  pure $ round . (* 1000000) . nominalDiffTimeToSeconds $ diffUTCTime now epoch

-- From https://www.williamyaoh.com/posts/2019-09-16-time-cheatsheet.html
mkUTCTime :: (Integer, Int, Int)
          -> (Int, Int, Pico)
          -> UTCTime
mkUTCTime (year, mon, day) (hours, mins, secs) =
  UTCTime (fromGregorian year mon day)
          (timeOfDayToTime (TimeOfDay hours mins secs))
