{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Log.Store.Internal where

import ByteString.StrictBuilder (builderBytes, word64BE)
-- import Control.Monad.Trans (lift)
-- import Control.Exception.Lifted (bracket)
-- import Control.Monad.Trans.Control (MonadBaseControl)
-- import Control.Monad.Trans.Resource (MonadUnliftIO, allocate, runResourceT)

import qualified Control.Concurrent.ReadWriteLock as RWL
import Control.Exception (bracket, throw, throwIO)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Atomics (atomicModifyIORefCAS)
import Data.Binary.Strict.Get (getWord64be, runGet)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.Default (def)
import Data.IORef (IORef)
import Data.List (isPrefixOf, sort)
import qualified Data.Text as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word64)
import qualified Database.RocksDB as R
import Log.Store.Exception
import Log.Store.Utils
import System.Directory (listDirectory)
import System.FilePath.Posix ((</>))

type LogName = T.Text

encodeLogName :: LogName -> B.ByteString
encodeLogName = encodeText

decodeLogName :: B.ByteString -> LogName
decodeLogName = decodeText

-- | Log Id
type LogID = Word64

maxLogIdKey :: B.ByteString
maxLogIdKey = "maxLogId"

encodeLogId :: LogID -> B.ByteString
encodeLogId = encodeWord64

decodeLogId :: B.ByteString -> LogID
decodeLogId = decodeWord64

-- | entry Id
type EntryID = Word64

minEntryId :: EntryID
minEntryId = 0

maxEntryId :: EntryID
maxEntryId = 0xffffffffffffffff

-- | key used when save entry to rocksdb
data EntryKey = EntryKey LogID EntryID
  deriving (Eq, Show)

encodeEntryKey :: EntryKey -> B.ByteString
encodeEntryKey (EntryKey logId entryId) =
  builderBytes $ word64BE logId `mappend` word64BE entryId

decodeEntryKey :: B.ByteString -> EntryKey
decodeEntryKey bs =
  if rem /= B.empty
    then throw $ LogStoreDecodeException "input error"
    else case res of
      Left s -> throw $ LogStoreDecodeException s
      Right v -> v
  where
    (res, rem) = decode' bs
    decode' = runGet $ do
      logId <- getWord64be
      EntryKey logId <$> getWord64be

-- | it is used to generate a new logId while
-- | creating a new log.
generateLogId :: MonadIO m => R.DB -> IORef LogID -> m LogID
generateLogId db logIdRef =
  liftIO $ do
    newId <- atomicModifyIORefCAS logIdRef (\curId -> (curId + 1, curId + 1))
    R.put db def maxLogIdKey (encodeWord64 newId)
    return newId

-- | generate entry Id
-- |
generateEntryId :: MonadIO m => IORef EntryID -> m EntryID
generateEntryId entryIdRef =
  liftIO $
    atomicModifyIORefCAS entryIdRef (\curId -> (curId + 1, curId + 1))

metaDbName :: String
metaDbName = "meta"

dataDbNamePrefix :: String
dataDbNamePrefix = "data-"

generateDataDbName :: MonadIO m => m String
generateDataDbName = liftIO $ do
  posixTime <- getPOSIXTime
  return $ dataDbNamePrefix ++ show (posixTimeToSeconds posixTime)

createDataDb :: MonadIO m => FilePath -> String -> Word64 -> m R.DB
createDataDb dbPath dbName cfWriteBufferSize =
  R.open
    R.defaultDBOptions
      { R.createIfMissing = True,
        R.writeBufferSize = cfWriteBufferSize,
        R.disableAutoCompactions = True,
        R.level0FileNumCompactionTrigger = -1,
        R.level0SlowdownWritesTrigger = -1,
        R.level0StopWritesTrigger = -1,
        R.softPendingCompactionBytesLimit = 18446744073709551615,
        R.hardPendingCompactionBytesLimit = 18446744073709551615
      }
    (dbPath </> dbName)

getFilesNumInDb :: MonadIO m => R.DB -> m Int
getFilesNumInDb db = liftIO $ do
  res <- R.getPropertyValue db "rocksdb.num-files-at-level0"
  case res of
    Nothing -> throwIO $ LogStoreIOException "getFilesNumInDb error"
    Just s -> do
      let parseRes = BC.readInt s
      case parseRes of
        Nothing -> throwIO $ LogStoreDecodeException "decode property value error"
        Just (num, leftStr) ->
          if B.null leftStr
            then return num
            else throwIO $ LogStoreDecodeException "decode property value error"

withDbReadOnly :: FilePath -> (R.DB -> IO a) -> IO a
withDbReadOnly dbPath =
  bracket
    ( R.openForReadOnly
        def
        dbPath
        False
    )
    R.close


getReadOnlyDataDbNames :: MonadIO m => FilePath -> RWL.RWLock -> m [FilePath]
getReadOnlyDataDbNames dbPath rwLock =
  liftIO $
    RWL.withRead
      rwLock
      ( do
          res <- listDirectory dbPath
          return $ init $ sort $ filter (isPrefixOf dataDbNamePrefix) res
      )
