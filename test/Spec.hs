{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import Conduit ((.|), ConduitT, runConduit, sinkList)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ReaderT, runReader, runReaderT)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Maybe (MaybeT, runMaybeT)
import Control.Monad.Trans.Reader (mapReaderT)
import Control.Monad.Trans.Resource (MonadResource, MonadUnliftIO, ResourceT, allocate, liftResourceT, runResourceT)
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
        ( withLogStoreTest
            ( do
                logHandle <-
                  open
                    "log"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                appendEntry logHandle "entry"
            )
        )
          `shouldReturn` Just 1
      it "put some entries to a log" $
        ( withLogStoreTest
            ( do
                logHandle <-
                  open
                    "log"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                entryId1 <- appendEntry logHandle "entry1"
                entryId2 <- appendEntry logHandle "entry2"
                entryId3 <- appendEntry logHandle "entry3"
                return [entryId1, entryId2, entryId3]
            )
        )
          `shouldReturn` Just [1, 2, 3]
      it "put some entries to multiple logs" $
        ( withLogStoreTest
            ( do
                lh1 <-
                  open
                    "log1"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log1EntryId1 <- appendEntry lh1 "log1-entry1"
                log1EntryId2 <- appendEntry lh1 "log1-entry2"
                log1EntryId3 <- appendEntry lh1 "log1-entry3"
                lh2 <-
                  open
                    "log2"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log2EntryId1 <- appendEntry lh2 "log2-entry1"
                log2EntryId2 <- appendEntry lh2 "log2-entry2"
                log2EntryId3 <- appendEntry lh2 "log2-entry3"
                lh3 <-
                  open
                    "log3"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log3EntryId1 <- appendEntry lh3 "log3-entry1"
                log3EntryId2 <- appendEntry lh3 "log3-entry2"
                log3EntryId3 <- appendEntry lh3 "log3-entry3"
                return
                  [ [log1EntryId1, log1EntryId2, log1EntryId3],
                    [log2EntryId1, log2EntryId2, log2EntryId3],
                    [log3EntryId1, log3EntryId2, log3EntryId3]
                  ]
            )
        )
          `shouldReturn` Just
            [ [1, 2, 3],
              [1, 2, 3],
              [1, 2, 3]
            ]
      it "put an entry to a log and read it" $
        ( withLogStoreTest
            ( do
                logHandle <-
                  open
                    "log"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                entryId <- appendEntry logHandle "entry"
                source <- readEntries logHandle Nothing Nothing
                liftIO $ runConduit $ source .| sinkList
            )
        )
          `shouldReturn` (Just ([Just "entry"]))
      it "put some entries to a log and read them" $
        ( withLogStoreTest
            ( do
                logHandle <-
                  open
                    "log"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                entryId1 <- appendEntry logHandle "entry1"
                entryId2 <- appendEntry logHandle "entry2"
                entryId3 <- appendEntry logHandle "entry3"
                source <- readEntries logHandle Nothing Nothing
                liftIO $ runConduit $ source .| sinkList
            )
        )
          `shouldReturn` Just [Just "entry1", Just "entry2", Just "entry3"]
      it "put some entries to multiple logs and read them" $
        ( withLogStoreTest
            ( do
                lh1 <-
                  open
                    "log1"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log1EntryId1 <- appendEntry lh1 "log1-entry1"
                log1EntryId2 <- appendEntry lh1 "log1-entry2"
                log1EntryId3 <- appendEntry lh1 "log1-entry3"
                lh2 <-
                  open
                    "log2"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log2EntryId1 <- appendEntry lh2 "log2-entry1"
                log2EntryId2 <- appendEntry lh2 "log2-entry2"
                log2EntryId3 <- appendEntry lh2 "log2-entry3"
                lh3 <-
                  open
                    "log3"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                log3EntryId1 <- appendEntry lh3 "log3-entry1"
                log3EntryId2 <- appendEntry lh3 "log3-entry2"
                log3EntryId3 <- appendEntry lh3 "log3-entry3"
                source1 <- readEntries lh1 Nothing Nothing
                r1 <- liftIO $ runConduit $ source1 .| sinkList
                source2 <- readEntries lh2 Nothing Nothing
                r2 <- liftIO $ runConduit $ source2 .| sinkList
                source3 <- readEntries lh3 Nothing Nothing
                r3 <- liftIO $ runConduit $ source3 .| sinkList
                return
                  [ r1,
                    r2,
                    r3
                  ]
            )
        )
          `shouldReturn` Just
            [ [Just "log1-entry1", Just "log1-entry2", Just "log1-entry3"],
              [Just "log2-entry1", Just "log2-entry2", Just "log2-entry3"],
              [Just "log3-entry1", Just "log3-entry2", Just "log3-entry3"]
            ]
      it "put many entries to a log" $
        ( withLogStoreTest
            ( do
                logHandle <-
                  open
                    "log"
                    defaultOpenOptions {writeMode = True, createIfMissing = True}
                appendEntries 1100 logHandle
            )
        )
          `shouldReturn` Just 1100

-- | help run test case
-- | wrap create temp directory
withLogStoreTest :: MonadUnliftIO m => ReaderT Context (MaybeT m) a -> m (Maybe a)
withLogStoreTest r =
  runResourceT
    ( do
        (_, path) <-
          createTempDirectory Nothing "log-store-test"
        (_, ctx) <-
          allocate
            (initialize $ UserDefinedEnv Config {rootDbPath = path})
            (runReaderT shutDown)
        runReaderT (mapReaderT (lift . runMaybeT) r) ctx
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
