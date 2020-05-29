{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import Conduit ((.|), ConduitT, runConduit, sinkList)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ReaderT, runReader, runReaderT)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Resource (MonadResource, MonadUnliftIO, allocate, liftResourceT, runResourceT)
import Data.Word (Word64)
import Log.Store.Base
  ( Config (..),
    Context,
    Entry,
    EntryID,
    Env (..),
    LogHandle (..),
    OpenOptions,
    appendEntry,
    close,
    createIfMissing,
    defaultOpenOptions,
    deserialize,
    initialize,
    liftIM1,
    liftIM2,
    liftIM3,
    open,
    readEntries,
    readMode,
    serialize,
    shutDown,
    writeMode,
  )
import System.IO.Temp (createTempDirectory)
import Test.Hspec
  ( describe,
    hspec,
    it,
    shouldReturn,
  )

main :: IO ()
main = hspec $ do
  describe "Basic Functionality" $
    do
      it "put an entry to a log" $
        ( exec
            ( do
                logHandle <-
                  open
                    "log"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                liftIM1 logHandle (`appendEntry` "entry")
            )
        )
          `shouldReturn` Just 1
      it "put some entries to a log" $
        ( exec
            ( do
                logHandle <-
                  open
                    "log"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                entryId1 <- liftIM1 logHandle (`appendEntry` "entry1")
                entryId2 <- liftIM1 logHandle (`appendEntry` "entry2")
                entryId3 <- liftIM1 logHandle (`appendEntry` "entry3")
                return [entryId1, entryId2, entryId3]
            )
        )
          `shouldReturn` [Just 1, Just 2, Just 3]
      it "put some entries to multiple logs" $
        ( exec
            ( do
                lh1 <-
                  open
                    "log1"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log1EntryId1 <- liftIM1 lh1 (`appendEntry` "entry1")
                log1EntryId2 <- liftIM1 lh1 (`appendEntry` "entry2")
                log1EntryId3 <- liftIM1 lh1 (`appendEntry` "entry3")
                lh2 <-
                  open
                    "log2"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log2EntryId1 <- liftIM1 lh2 (`appendEntry` "entry1")
                log2EntryId2 <- liftIM1 lh2 (`appendEntry` "entry2")
                log2EntryId3 <- liftIM1 lh2 (`appendEntry` "entry3")
                lh3 <-
                  open
                    "log3"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log3EntryId1 <- liftIM1 lh3 (`appendEntry` "entry1")
                log3EntryId2 <- liftIM1 lh3 (`appendEntry` "entry2")
                log3EntryId3 <- liftIM1 lh3 (`appendEntry` "entry3")
                return
                  [ [log1EntryId1, log1EntryId2, log1EntryId3],
                    [log2EntryId1, log2EntryId2, log2EntryId3],
                    [log3EntryId1, log3EntryId2, log3EntryId3]
                  ]
            )
        )
          `shouldReturn` [ [Just 1, Just 2, Just 3],
                           [Just 1, Just 2, Just 3],
                           [Just 1, Just 2, Just 3]
                         ]
      it "put an entry to a log and read it" $
        ( exec
            ( do
                logHandle <-
                  open
                    "log"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                entryId <- liftIM1 logHandle (`appendEntry` "entry")
                source <- liftIM3 logHandle entryId entryId readEntries
                liftIM1
                  source
                  ( \s -> liftIO $ do
                      r <- runConduit $ s .| sinkList
                      return $ Just r
                  )
            )
        )
          `shouldReturn` (Just ([Just "entry"]))
      it "put some entries to a log and read them" $
        ( exec
            ( do
                logHandle <-
                  open
                    "log"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                entryId1 <- liftIM1 logHandle (`appendEntry` "entry1")
                entryId2 <- liftIM1 logHandle (`appendEntry` "entry2")
                entryId3 <- liftIM1 logHandle (`appendEntry` "entry3")
                source <- liftIM3 logHandle entryId1 entryId3 readEntries
                liftIM1
                  source
                  ( \s -> liftIO $ do
                      r <- runConduit $ s .| sinkList
                      return $ Just r
                  )
            )
        )
          `shouldReturn` (Just ([Just "entry1", Just "entry2", Just "entry3"]))
      it "put some entries to multiple logs and read them" $
        ( exec
            ( do
                lh1 <-
                  open
                    "log1"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log1EntryId1 <- liftIM1 lh1 (`appendEntry` "entry1")
                log1EntryId2 <- liftIM1 lh1 (`appendEntry` "entry2")
                log1EntryId3 <- liftIM1 lh1 (`appendEntry` "entry3")
                lh2 <-
                  open
                    "log2"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log2EntryId1 <- liftIM1 lh2 (`appendEntry` "entry1")
                log2EntryId2 <- liftIM1 lh2 (`appendEntry` "entry2")
                log2EntryId3 <- liftIM1 lh2 (`appendEntry` "entry3")
                lh3 <-
                  open
                    "log3"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log3EntryId1 <- liftIM1 lh3 (`appendEntry` "entry1")
                log3EntryId2 <- liftIM1 lh3 (`appendEntry` "entry2")
                log3EntryId3 <- liftIM1 lh3 (`appendEntry` "entry3")
                source1 <- liftIM3 lh1 log1EntryId1 log1EntryId3 readEntries
                r1 <-
                  liftIM1
                    source1
                    ( \s -> liftIO $ do
                        r <- runConduit $ s .| sinkList
                        return $ Just r
                    )
                source2 <- liftIM3 lh2 log2EntryId1 log2EntryId3 readEntries
                r2 <-
                  liftIM1
                    source2
                    ( \s -> liftIO $ do
                        r <- runConduit $ s .| sinkList
                        return $ Just r
                    )
                source3 <- liftIM3 lh3 log3EntryId1 log3EntryId3 readEntries
                r3 <-
                  liftIM1
                    source3
                    ( \s -> liftIO $ do
                        r <- runConduit $ s .| sinkList
                        return $ Just r
                    )
                return
                  [ r1,
                    r2,
                    r3
                  ]
            )
        )
          `shouldReturn` [ Just ([Just "entry1", Just "entry2", Just "entry3"]),
                           Just ([Just "entry1", Just "entry2", Just "entry3"]),
                           Just ([Just "entry1", Just "entry2", Just "entry3"])
                         ]
      it "put many entries to a log" $
        ( exec
            ( do
                logHandle <-
                  open
                    "log"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                liftIM1 logHandle (appendEntries 1100)
            )
        )
          `shouldReturn` Just 1100

-- | help run test case
-- | wrap create temp directory
exec r =
  runResourceT
    ( do
        (_, path) <-
          createTempDirectory
            Nothing
            $ "log-store-test"
        (_, ctx) <-
          allocate
            (initialize $ UserDefinedEnv Config {rootDbPath = path})
            (runReaderT shutDown)
        runReaderT r ctx
    )

-- | append n entries to a log
appendEntries n lh = append' 1
  where
    append' x =
      if (x == n)
        then do
          id <- appendEntry lh "entry"
          -- liftIO $ print id
          return id
        else do
          id <- appendEntry lh "entry"
          -- liftIO $ print id
          append' (x + 1)
