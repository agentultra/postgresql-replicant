module Main where

import Data.Aeson
import Data.Maybe
import Control.Exception
import System.Environment

import Database.PostgreSQL.Replicant

main :: IO ()
main = do
  dbUser <- fromMaybe "postgresql" <$> lookupEnv "PG_USER"
  dbName <- fromMaybe "postgresql" <$> lookupEnv "PG_DATABASE"
  dbHost <- fromMaybe "localhost" <$> lookupEnv "PG_HOST"
  dbPort <- fromMaybe "5432" <$> lookupEnv "PG_PORT"
  dbSlot <- fromMaybe "replicant_test" <$> lookupEnv "PG_SLOTNAME"
  dbUpdateDelay <- fromMaybe "3000" <$> lookupEnv "PG_UPDATEDELAY"
  let settings = PgSettings dbUser Nothing dbName dbHost dbPort dbSlot dbUpdateDelay
  withLogicalStream settings $ \changePayload -> do
    putStrLn "Change received!"
    print $ encode changePayload
    case changePayload of
      InformationMessage infoMsg ->
        pure Nothing
      ChangeMessage change ->
        pure . Just $ changeNextLSN change
  `catch`
  \exc -> do
    putStrLn "Something bad happened: "
    print @SomeException exc
