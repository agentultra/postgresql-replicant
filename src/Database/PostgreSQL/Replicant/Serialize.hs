{-# LANGUAGE Strict #-}

module Database.PostgreSQL.Replicant.Serialize where

import Data.Binary.Get
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL

-- | Consume the rest of the @Get a@ input as a strict ByteString
consumeByteStringToEnd :: Get ByteString
consumeByteStringToEnd = BL.toStrict <$> getRemainingLazyByteString
