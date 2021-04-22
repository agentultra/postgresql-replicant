module Main where

import Control.Exception
import Database.PostgreSQL.LibPQ
import Lib
import Database.PostgreSQL.Replicant.Protocol

main :: IO ()
main = do
  let settings = PgSettings "jking" "hackday" "localhost" "5432" "hackday_sub_1"
  withConnection settings $ \change -> do
    putStrLn "Change received!"
    print change
  `catch`
  \exc -> do
    putStrLn "Something bad happened: "
    print @SomeException exc
