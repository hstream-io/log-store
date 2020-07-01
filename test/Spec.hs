{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Reader (ReaderT, runReaderT)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Resource (MonadUnliftIO, allocate, runResourceT)
import Log.Store.Base
  ( Config (..),
    Context,
    Env (..),
    LogHandle (..),
    appendEntry,
    create,
    createIfMissing,
    defaultOpenOptions,
    initialize,
    open,
    readEntries,
    readMode,
    shutDown,
    writeMode,
  )
import qualified Streamly.Prelude as S
import System.IO.Temp (createTempDirectory)
import Test.Hspec
  ( describe,
    hspec,
    it,
    shouldReturn,
  )

main :: IO ()
main = hspec $
  describe "Basic Functionality" $
    do
      it "create logs" $
        withLogStoreTest
          ( do
              log1 <- create "log1"
              log2 <- create "log2"
              log3 <- create "log3"
              return [log1, log2, log3]
          )
          `shouldReturn` [1, 2, 3]
      it "put an entry to a log" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              appendEntry logHandle "entry"
          )
          `shouldReturn` 1
      it "put some entries to a log" $
        withLogStoreTest
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
          `shouldReturn` [1, 2, 3]
      it "put some entries to multiple logs" $
        withLogStoreTest
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
          `shouldReturn` [[1, 2, 3], [1, 2, 3], [1, 2, 3]]
      it "put an entry to a log and read it" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              entryId <- appendEntry logHandle "entry"
              s <- readEntries logHandle Nothing Nothing
              liftIO $ S.toList s
          )
          `shouldReturn` [("entry", 1)]
      it "put some entries to a log and read them" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              entryId1 <- appendEntry logHandle "entry1"
              entryId2 <- appendEntry logHandle "entry2"
              entryId3 <- appendEntry logHandle "entry3"
              s <- readEntries logHandle Nothing Nothing
              liftIO $ S.toList s
          )
          `shouldReturn` [("entry1", 1), ("entry2", 2), ("entry3", 3)]
      it "put some entries to multiple logs and read them" $
        withLogStoreTest
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
              s1 <- readEntries lh1 Nothing Nothing
              r1 <- liftIO $ S.toList s1
              s2 <- readEntries lh2 Nothing Nothing
              r2 <- liftIO $ S.toList s2
              s3 <- readEntries lh3 Nothing Nothing
              r3 <- liftIO $ S.toList s3
              return [r1, r2, r3]
          )
          `shouldReturn` [ [("log1-entry1", 1), ("log1-entry2", 2), ("log1-entry3", 3)],
                           [("log2-entry1", 1), ("log2-entry2", 2), ("log2-entry3", 3)],
                           [("log3-entry1", 1), ("log3-entry2", 2), ("log3-entry3", 3)]
                         ]
      it "put many entries to a log" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              appendEntries 4097 logHandle
          )
          `shouldReturn` 4097

-- | help run test case
-- | wrap create temp directory
withLogStoreTest :: MonadUnliftIO m => ReaderT Context m a -> m a
withLogStoreTest r =
  runResourceT
    ( do
        (_, path) <-
          createTempDirectory Nothing "log-store-test"
        (_, ctx) <-
          allocate
            (initialize $ UserDefinedEnv Config {rootDbPath = path})
            (runReaderT shutDown)
        lift $ runReaderT r ctx
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
