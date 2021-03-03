module Database.PostgreSQL.Replicant.Serialize where

import Data.ByteString (ByteString)
import Data.Serialize

-- | Consume the rest of the @Get a@ input as a ByteString
consumeByteStringToEnd :: Get ByteString
consumeByteStringToEnd = do
  numRemaining <- remaining
  getByteString numRemaining
