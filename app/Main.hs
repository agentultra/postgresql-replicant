module Main where

import Data.Aeson
import Data.Maybe
import Control.Exception
import System.Environment

import Database.PostgreSQL.Replicant

main :: IO ()
main = do
  dbUser <- maybe "postgresql" id <$> lookupEnv "PG_USER"
  dbName <- maybe "postgresql" id <$> lookupEnv "PG_DATABASE"
  dbHost <- maybe "localhost" id <$> lookupEnv "PG_HOST"
  dbPort <- maybe "5432" id <$> lookupEnv "PG_PORT"
  dbSlot <- maybe "replicant_test" id <$> lookupEnv "PG_SLOTNAME"
  dbUpdateDelay <- maybe "3000" id <$> lookupEnv "PG_UPDATEDELAY"
  let settings = PgSettings dbUser dbName dbHost dbPort dbSlot dbUpdateDelay
  withLogicalStream settings $ \change -> do
    putStrLn "Change received!"
    print $ encode change
    pure $ changeNextLSN change
  `catch`
  \exc -> do
    putStrLn "Something bad happened: "
    print @SomeException exc
