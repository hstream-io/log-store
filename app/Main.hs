{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Reader (ReaderT)
import qualified Data.ByteString as B
import qualified Data.Vector as V
import Data.Word (Word32, Word64)
import Log.Store.Base
import qualified Streamly.Prelude as S
import System.Console.CmdArgs.Implicit

data Options
  = Append
      { dbPath :: FilePath,
        logName :: LogName,
        totalSize :: Int,
        batchSize :: Int,
        entrySize :: Int
      }
  | Read
      { dbPath :: FilePath,
        logName :: LogName
      }
  deriving (Show, Data, Typeable)

appendOpts =
  Append
    { totalSize = 1 &= help "total kv sizes ready to append (GB)",
      batchSize = 64 &= help "number of entries in a batch",
      entrySize = 128 &= help "size of each entry (byte)",
      dbPath = "/tmp/rocksdb" &= help "which to store data",
      logName = "log" &= help "name of log to write"
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
    Append {..} ->
      withLogStore
        Config
          { rootDbPath = dbPath,
            dataCfWriteBufferSize = 1024 * 1024 * 1024,
            dbWriteBufferSize = 0,
            enableDBStatistics = True,
            dbStatsDumpPeriodSec = 10
          }
        ( do
            lh <- open logName defaultOpenOptions {createIfMissing = True, writeMode = True}
            let realEntrySize = entrySize + 128
            let batchNum = (totalSize * 1024 * 1024 * 1024) `div` (batchSize * entrySize)
            writeNBytesEntriesBatch lh entrySize batchSize batchNum
            return ()
        )
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
            readAll lh
        )

readAll :: MonadIO m => LogHandle -> ReaderT Context m ()
readAll lh = do
  stream <- readEntries lh Nothing Nothing
  liftIO $ S.drain stream

nBytesEntry :: Int -> B.ByteString
nBytesEntry n = B.replicate n 0xff

writeNBytesEntriesBatch ::
  MonadIO m =>
  LogHandle ->
  Int ->
  Int ->
  Int ->
  ReaderT Context m (V.Vector EntryID)
writeNBytesEntriesBatch lh entrySize batchSize batchNum = write' lh 1
  where
    write' lh x =
      if x == batchNum
        then appendEntries lh $ V.replicate batchSize entry
        else do
          appendEntries lh $ V.replicate batchSize entry
          write' lh (x + 1)
    entry = nBytesEntry entrySize
