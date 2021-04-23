module Database.PostgreSQL.Replicant.Util where

import Control.Exception
import GHC.Int

mkInt64 :: Int -> Int64
mkInt64 k = fromIntegral k * 1000000

maybeThrow :: Exception e => e -> Maybe a -> IO a
maybeThrow exc = \case
  Nothing -> throwIO exc
  Just x  -> pure x
