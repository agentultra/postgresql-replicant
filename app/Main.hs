module Main where

import Control.Exception
import Database.PostgreSQL.LibPQ
import Database.PostgreSQL.Replicant.Protocol

main :: IO ()
main = do
  c <- connectdb "user=jking dbname=hackday host=localhost port=5432 replication=database"
  mInfo <- identifySystemSync c
  case mInfo of
    Nothing -> error "Error identifying system"
    Just info -> do
      mRepSlot <- createReplicationSlotSync c "hackday_sub_1"
      case mRepSlot of
        Nothing -> error "Error creating replication slot"
        Just repSlot ->
          startReplicationStream c (replicationSlotName repSlot) (identifySystemLogPos info)
          `catch`
          \exc -> do
            putStrLn "Something bad happened: "
            print @SomeException exc
