module Main where

import Database.PostgreSQL.LibPQ
import Database.PostgreSQL.Replicant.Protocol

main :: IO ()
main = do
  c <- connectdb "user=jking dbname=hackday host=localhost port=5432 replication=database"
  (Just info) <- identifySystemSync c
  (Just repSlot) <- createReplicationSlotSync c "hackday_sub_1"
  startReplicationStream c (replicationSlotName repSlot) (identifySystemLogPos info)
