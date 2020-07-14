{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Concurrent.Async.Lifted.Safe (async, wait)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ReaderT, runReaderT)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Resource (MonadUnliftIO, allocate, runResourceT)
import qualified Data.ByteString as B
import qualified Data.ByteString.UTF8 as U
import Data.Function ((&))
import qualified Data.Vector as V
import Log.Store.Base
  ( Config (..),
    Context,
    LogHandle (..),
    appendEntries,
    appendEntry,
    create,
    createIfMissing,
    defaultOpenOptions,
    initialize,
    open,
    readEntries,
    readMode,
    shutDown,
    withLogStore,
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
      --      it "append entry repeatly to a log and read them" $
      --        withLogStore Config {rootDbPath = "db-temp"}
      --          ( do
      --              openAndAppendForever
      --              return "success"
      --          )
      --          `shouldReturn` "success"
      it "create logs" $
        withLogStoreTest
          ( do
              log1 <- create "log1"
              log2 <- create "log2"
              log3 <- create "log3"
              return [log1, log2, log3]
          )
          `shouldReturn` [1, 2, 3]
      it "open an existent log" $
        withLogStoreTest
          ( do
              create "log"
              open
                "log"
                defaultOpenOptions {writeMode = True}
              return "success"
          )
          `shouldReturn` "success"
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
      it "append entries to a log and read them " $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              appendEntries logHandle (V.replicate 3 (U.fromString "entry"))
              s <- readEntries logHandle Nothing Nothing
              liftIO $ S.toList s
          )
          `shouldReturn` [("entry", 1), ("entry", 2), ("entry", 3)]
      it "append entry repeatly to a log and read them" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              appendEntryRepeat 300 logHandle
              s <- readEntries logHandle Nothing Nothing
              liftIO $ s & S.map snd & S.toList
          )
          `shouldReturn` [1 .. 300]
      it "multiple open" $
        withLogStoreTest
          ( do
              lh1 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              appendEntryRepeat 300 lh1
              lh2 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True}
              appendEntryRepeat 300 lh2
              lh3 <-
                open
                  "log"
                  defaultOpenOptions
              s <- readEntries lh3 Nothing Nothing
              liftIO $ s & S.map snd & S.toList
          )
          `shouldReturn` [1 .. 600]
      it "sequencial open the same log should return the same logHandle" $
        withLogStoreTest
          ( do
              lh1 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              lh2 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              lh3 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              return $ lh1 == lh2 && lh1 == lh3
          )
          `shouldReturn` True
      it "concurrent open the same log should return the same logHandle" $
        withLogStoreTest
          ( do
              c1 <-
                async
                  ( open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  )
              c2 <-
                async
                  ( open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  )
              c3 <-
                async
                  ( open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  )
              r1 <- wait c1
              r2 <- wait c2
              r3 <- wait c3
              return $ r1 == r2 && r1 == r2
          )
          `shouldReturn` True
      it "concurrent open, append and read different logs" $
        withLogStoreTest
          ( do
              c1 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log1"
                          defaultOpenOptions {writeMode = True, createIfMissing = True}
                      appendEntryRepeat 300 logHandle
                      s <- readEntries logHandle Nothing Nothing
                      liftIO $ s & S.map snd & S.toList
                  )
              c2 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log2"
                          defaultOpenOptions {writeMode = True, createIfMissing = True}
                      appendEntryRepeat 300 logHandle
                      s <- readEntries logHandle Nothing Nothing
                      liftIO $ s & S.map snd & S.toList
                  )
              c3 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log3"
                          defaultOpenOptions {writeMode = True, createIfMissing = True}
                      appendEntryRepeat 300 logHandle
                      s <- readEntries logHandle Nothing Nothing
                      liftIO $ s & S.map snd & S.toList
                  )
              r1 <- wait c1
              r2 <- wait c2
              r3 <- wait c3
              return [r1, r2, r3]
          )
          `shouldReturn` [[1 .. 300], [1 .. 300], [1 .. 300]]

-- | append n entries to a log
appendEntryRepeat n lh = append' 1
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

appendForever lh = do
  appendEntries lh $ V.replicate 128 $ B.replicate 4096 0xff
  appendForever lh

openAndAppendForever :: MonadIO m => ReaderT Context m ()
openAndAppendForever = do
  lh <-
    open
      "log"
      defaultOpenOptions {writeMode = True, createIfMissing = True}
  appendEntries lh $ V.replicate 128 $ B.replicate 4096 0xff
  openAndAppendForever

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
            (initialize $ Config {rootDbPath = path})
            (runReaderT shutDown)
        lift $ runReaderT r ctx
    )
