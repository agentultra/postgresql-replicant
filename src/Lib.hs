module Lib
    ( someFunc
    ) where

import Database.PostgreSQL.LibPQ
import Database.PostgreSQL.Replicant.Message

someFunc :: IO ()
someFunc = putStrLn "someFunc"

withConnection :: Connection -> (Maybe ReplicationMessage -> IO ()) -> IO ()
withConnection = undefined
