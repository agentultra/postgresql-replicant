{-# LANGUAGE Strict #-}

module Database.PostgreSQL.Replicant.Settings where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B

data PgSettings
  = PgSettings
  { pgUser        :: String
  , pgPassword    :: Maybe String
  , pgDbName      :: String
  , pgHost        :: String
  , pgPort        :: String
  , pgSlotName    :: String
  , pgUpdateDelay :: String -- ^ Controls how frequently the
                            -- primaryKeepAlive thread updates
                            -- PostgresSQL in @ms@
  }
  deriving (Eq, Show)

pgConnectionString :: PgSettings -> ByteString
pgConnectionString PgSettings {..} = B.intercalate " "
  [ "user=" <> B.pack pgUser
  , maybe "" (\pgPass -> "pass=" <> B.pack pgPass) pgPassword
  , "dbname=" <> B.pack pgDbName
  , "host=" <> B.pack pgHost
  , "port=" <> B.pack pgPort
  , "replication=database"
  ]
