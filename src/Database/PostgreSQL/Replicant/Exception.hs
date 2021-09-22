{-# LANGUAGE Strict #-}

module Database.PostgreSQL.Replicant.Exception where

import Control.Exception
import Data.Typeable

newtype ReplicantException = ReplicantException String
  deriving (Show, Typeable)

instance Exception ReplicantException
