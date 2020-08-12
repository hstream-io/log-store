{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

module Main where

import Control.Concurrent.Async.Lifted.Safe (async, wait)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ReaderT, runReaderT)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Resource (MonadUnliftIO, allocate, runResourceT)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.UTF8 as U
import qualified Data.Foldable as F
import Data.Function ((&))
import Data.List (sort)
import qualified Data.Vector as V
import Data.Word (Word64)
import Log.Store.Base
  ( Config (..),
    Context,
    Entry,
    EntryID,
    LogHandle,
    appendEntries,
    appendEntry,
    createIfMissing,
    defaultConfig,
    defaultOpenOptions,
    initialize,
    open,
    readEntries,
    readMode,
    shutDown,
    withLogStore,
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
main = hspec $
  describe "Basic Functionality" $
    do
      it "open logs for creating" $
        withLogStoreTest
          ( do
              open "log1" defaultOpenOptions {writeMode = True, createIfMissing = True}
              open "log2" defaultOpenOptions {writeMode = True, createIfMissing = True}
              open "log3" defaultOpenOptions {writeMode = True, createIfMissing = True}
              return "success"
          )
          `shouldReturn` "success"
      it "open an existent log" $
        withLogStoreTest
          ( do
              open "log" defaultOpenOptions {writeMode = True, createIfMissing = True}
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
              F.toList <$> readEntries logHandle Nothing Nothing
          )
          `shouldReturn` [(1, "entry")]
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
              F.toList <$> readEntries logHandle Nothing Nothing
          )
          `shouldReturn` [(1, "entry1"), (2, "entry2"), (3, "entry3")]
      it "put some entries to multiple logs and read them (1)" $
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
              r1 <- readEntries lh1 Nothing Nothing
              r2 <- readEntries lh2 Nothing Nothing
              r3 <- readEntries lh3 Nothing Nothing
              return $ fmap F.toList [r1, r2, r3]
          )
          `shouldReturn` [ [(1, "log1-entry1"), (2, "log1-entry2"), (3, "log1-entry3")],
                           [(1, "log2-entry1"), (2, "log2-entry2"), (3, "log2-entry3")],
                           [(1, "log3-entry1"), (2, "log3-entry2"), (3, "log3-entry3")]
                         ]
      it "put some entries to multiple logs and read them (2)" $
        withLogStoreTest
          ( do
              lh1 <-
                open
                  "log1"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              lh2 <-
                open
                  "log2"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              appendEntry lh2 "log2-entry1"
              appendEntry lh2 "log2-entry2"
              appendEntry lh2 "log2-entry3"
              F.toList <$> readEntries lh1 Nothing Nothing
          )
          `shouldReturn` []
      it "append entries to a log and read them " $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              appendEntries logHandle (V.replicate 3 (U.fromString "entry"))
              F.toList <$> readEntries logHandle Nothing Nothing
          )
          `shouldReturn` [(1, "entry"), (2, "entry"), (3, "entry")]
      it "append entry repeatly to a log and read them" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              appendEntryRepeat 300 logHandle ""
              res <- F.toList <$> readEntries logHandle Nothing Nothing
              return $ map fst res
          )
          `shouldReturn` [1 .. 300]
      it "multiple open" $
        withLogStoreTest
          ( do
              lh1 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              appendEntryRepeat 300 lh1 ""
              lh2 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              appendEntryRepeat 300 lh2 ""
              lh3 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              res <- F.toList <$> readEntries lh3 Nothing Nothing
              return $ map fst res
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
              open
                "log"
                defaultOpenOptions {writeMode = True, createIfMissing = True}
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
                      appendEntryRepeat 3 logHandle "l1"
                      F.toList <$> readEntries logHandle Nothing Nothing
                  )
              c2 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log2"
                          defaultOpenOptions {writeMode = True, createIfMissing = True}
                      appendEntryRepeat 3 logHandle "l2"
                      F.toList <$> readEntries logHandle Nothing Nothing
                  )
              c3 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log3"
                          defaultOpenOptions {writeMode = True, createIfMissing = True}
                      appendEntryRepeat 3 logHandle "l3"
                      F.toList <$> readEntries logHandle Nothing Nothing
                  )
              r1 <- wait c1
              r2 <- wait c2
              r3 <- wait c3
              return [r1, r2, r3]
          )
          `shouldReturn` [generateReadResult 3 "l1", generateReadResult 3 "l2", generateReadResult 3 "l3"]
      it "concurrent append to the same log" $
        withLogStoreTest
          ( do
              open
                "log"
                defaultOpenOptions {writeMode = True, createIfMissing = True}
              c1 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log"
                          defaultOpenOptions {writeMode = True}
                      appendEntryRepeat 300 logHandle ""
                      return logHandle
                  )
              c2 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log"
                          defaultOpenOptions {writeMode = True}
                      appendEntryRepeat 300 logHandle ""
                      return logHandle
                  )
              c3 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log"
                          defaultOpenOptions {writeMode = True}
                      appendEntryRepeat 300 logHandle ""
                      return logHandle
                  )
              wait c1
              wait c2
              lh <- wait c3
              res <- F.toList <$> readEntries lh Nothing Nothing
              return $ map fst res
          )
          `shouldReturn` [1 .. 900]

-- | append n entries to a log
appendEntryRepeat :: MonadIO m => Int -> LogHandle -> String -> ReaderT Context m EntryID
appendEntryRepeat n lh entryPrefix = append' 1
  where
    append' x =
      if (x == n)
        then do
          id <- appendEntry lh $ C.pack $ entryPrefix ++ testEntryContent
          -- liftIO $ print id
          return id
        else do
          id <- appendEntry lh $ C.pack $ entryPrefix ++ testEntryContent
          -- liftIO $ print id
          append' (x + 1)

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
            ( initialize defaultConfig {rootDbPath = path}
            )
            (runReaderT shutDown)
        lift $ runReaderT r ctx
    )

generateReadResult :: Word64 -> String -> [(EntryID, Entry)]
generateReadResult num entryPrefix = map (,C.pack $ entryPrefix ++ testEntryContent) [1 .. num]

testEntryContent :: String
testEntryContent = "entry"
