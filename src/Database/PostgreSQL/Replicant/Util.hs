module Database.PostgreSQL.Replicant.Util where

import GHC.Int

mkInt64 :: Int -> Int64
mkInt64 k = fromIntegral k * 1000000
