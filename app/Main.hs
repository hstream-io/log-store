{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.Async.Lifted (Async, async, mapConcurrently_, wait)
import Control.Exception (throwIO)
import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Reader (ReaderT)
import Data.Atomics (atomicModifyIORefCAS)
import qualified Data.ByteString as B
import Data.Function ((&))
import qualified Data.HashMap.Strict as H
import Data.IORef (IORef, newIORef)
import qualified Data.Text as T
import qualified Data.Vector as V
import Data.Word (Word32, Word64)
import Log.Store.Base
import Streamly.Data.Fold (Fold)
import qualified Streamly.Internal.Data.Fold as FL
import qualified Streamly.Prelude as S
import System.Clock (Clock (Monotonic), TimeSpec (..), getTime)
import System.Console.CmdArgs.Implicit

data Options
  = Append
      { dbPath :: FilePath,
        logNamePrefix :: T.Text,
        totalSize :: Int,
        batchSize :: Int,
        entrySize :: Int,
        logNum :: Int
      }
  | Read
      { dbPath :: FilePath,
        logName :: LogName
      }
  deriving (Show, Data, Typeable)

appendOpts =
  Append
    { totalSize = 100 &= help "total kv sizes ready to append to a single log (MB)",
      batchSize = 64 &= help "number of entries in a batch",
      entrySize = 128 &= help "size of each entry (byte)",
      dbPath = "/tmp/rocksdb" &= help "which to store data",
      logNamePrefix = "log" &= help "name prefix of logs to write",
      logNum = 1 &= help "num of log to write"
    }
    &= help "append"

readOpts =
  Read
    { dbPath = "/tmp/rocksdb" &= help "db path",
      logName = "log" &= help "name of log to read"
    }
    &= help "read"

main :: IO ()
main = do
  opts <- cmdArgs (modes [appendOpts, readOpts])
  case opts of
    Append {..} -> do
      numRef <- newIORef (0 :: Integer)
      let dict = H.singleton appendedEntryNumKey numRef
      printAppendSpeed dict entrySize 0
      withLogStore
        Config
          { rootDbPath = dbPath,
            dataCfWriteBufferSize = 1024 * 1024 * 1024,
            dbWriteBufferSize = 0,
            enableDBStatistics = True,
            dbStatsDumpPeriodSec = 2
          }
        (mapConcurrently_ (appendTask dict totalSize entrySize batchSize . T.append logNamePrefix . T.pack . show) [1 .. logNum])
    Read {..} ->
      withLogStore
        Config
          { rootDbPath = dbPath,
            dataCfWriteBufferSize = 1024 * 1024 * 1024,
            dbWriteBufferSize = 0,
            enableDBStatistics = True,
            dbStatsDumpPeriodSec = 30
          }
        ( do
            lh <- open logName defaultOpenOptions
            -- drainAll lh
            drainBatch 1024 lh
        )

appendTask ::
  MonadIO m =>
  H.HashMap B.ByteString (IORef Integer) ->
  Int ->
  Int ->
  Int ->
  LogName ->
  ReaderT Context m ()
appendTask dict totalSize entrySize batchSize logName = do
  lh <- open logName defaultOpenOptions {createIfMissing = True, writeMode = True}
  let batchNum = (totalSize * 1024 * 1024) `div` (batchSize * entrySize)
  writeNBytesEntriesBatch dict lh entrySize batchSize batchNum
  return ()

-- record read speed
drainAll :: MonadIO m => LogHandle -> ReaderT Context m ()
drainAll lh = do
  stream <- readEntries lh Nothing Nothing
  -- liftIO $ S.drain stream
  liftIO $ S.mapM_ (print . fromIntegral) $ S.intervalsOf 1 FL.length stream

drainBatch :: MonadIO m => EntryID -> LogHandle -> ReaderT Context m ()
drainBatch batchSize lh = drainBatch' 1 batchSize TimeSpec {sec = 0, nsec = 0} 0
  where
    sampleDuration = TimeSpec {sec = 1, nsec = 0}

    drainBatch' :: MonadIO m => EntryID -> EntryID -> TimeSpec -> Word64 -> ReaderT Context m ()
    drainBatch' start end remainingTime accItem = do
      startTime <- liftIO $ getTime Monotonic
      stream <- readEntries lh (Just start) (Just end)
      readNum <- liftIO $ S.length stream
      endTime <- liftIO $ getTime Monotonic
      let duration = endTime - startTime + remainingTime
      let total = accItem + fromIntegral readNum
      if duration >= sampleDuration
        then do
          liftIO $ print total
          drainBatch' (end + 1) (end + batchSize) 0 0
        else drainBatch' (end + 1) (end + batchSize) duration total

nBytesEntry :: Int -> B.ByteString
nBytesEntry n = B.replicate n 0xff

appendedEntryNumKey :: B.ByteString
appendedEntryNumKey = "appendedEntryNum"

writeNBytesEntriesBatch ::
  MonadIO m =>
  H.HashMap B.ByteString (IORef Integer) ->
  LogHandle ->
  Int ->
  Int ->
  Int ->
  ReaderT Context m ()
writeNBytesEntriesBatch dict lh entrySize batchSize batchNum = write' lh 1
  where
    write' lh x =
      if x == batchNum
        then do
          appendEntries lh $ V.replicate batchSize entry
          increaseBy dict appendedEntryNumKey $ toInteger batchSize
          return ()
        else do
          appendEntries lh $ V.replicate batchSize entry
          increaseBy dict appendedEntryNumKey $ toInteger batchSize
          write' lh (x + 1)
    entry = nBytesEntry entrySize

increaseBy ::
  MonadIO m =>
  H.HashMap B.ByteString (IORef Integer) ->
  B.ByteString ->
  Integer ->
  m Integer
increaseBy dict key num = liftIO $ do
  let r = H.lookup key dict
  case r of
    Nothing -> throwIO $ userError "error"
    Just v ->
      atomicModifyIORefCAS v (\curNum -> (curNum + num, curNum + num))

periodRun :: Int -> Int -> IO a -> IO ()
periodRun initDelay interval action = do
  forkIO $ do
    threadDelay $ initDelay * 1000
    forever $ do
      threadDelay $ interval * 1000
      action
  return ()

printAppendSpeed :: H.HashMap B.ByteString (IORef Integer) -> Int -> Integer -> IO ()
printAppendSpeed dict entrySize lastNum = do
  forkIO $ do
    threadDelay $ 1000000
    forever $ printSpeed 0
  return ()
  where
    printSpeed num = do
      threadDelay 1000000
      curNum <- increaseBy dict appendedEntryNumKey 0
      print $ fromInteger ((curNum - num) * toInteger entrySize) / 1024 / 1024
      printSpeed curNum
