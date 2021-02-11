module Main where

import Database.PostgreSQL.LibPQ
import Database.PostgreSQL.Replicant.Protocol

main :: IO ()
main = do
  c <- connectdb "user=jking dbname=hackday2 host=localhost port=5432 replication=database"
  (Just info) <- identifySystemSync c
  (Just repSlot) <- createReplicationSlotSync c "hackday2_sub_3"
  startReplicationStream c (replicationSlotName repSlot) (identifySystemLogPos info)
