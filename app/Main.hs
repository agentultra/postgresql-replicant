module Main where

import Control.Exception
import Database.PostgreSQL.Replicant

main :: IO ()
main = do
  let settings = PgSettings "postgresql" "postgresql" "localhost" "5432" "replicant_test"
  withLogicalStream settings $ \change -> do
    putStrLn "Change received!"
    print change
  `catch`
  \exc -> do
    putStrLn "Something bad happened: "
    print @SomeException exc
