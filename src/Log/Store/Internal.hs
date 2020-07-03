{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Log.Store.Internal where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString as B
import Data.Default (def)
import Data.IORef (IORef, readIORef, writeIORef)
import Data.Store (Store, encode)
import Data.Word (Word64)
import qualified Database.RocksDB as R
import GHC.Generics (Generic)
import Log.Store.Utils

-- | Log Id
type LogID = Word64

-- | entry Id
type EntryID = Word64

minEntryId :: EntryID
minEntryId = 0

maxEntryId :: EntryID
maxEntryId = 0xffffffffffffffff

firstNormalEntryId :: EntryID
firstNormalEntryId = 1

-- | entry content with some meta info
data InnerEntry = InnerEntry EntryID B.ByteString
  deriving (Generic, Eq, Show)

instance Store InnerEntry

-- | key used when save entry to rocksdb
data EntryKey = EntryKey LogID EntryID
  deriving (Generic, Eq, Show)

instance Store EntryKey

-- | it is used to generate a new logId while
-- | creating a new log.
-- |
-- | todo:
-- | exception need to consider
generateLogId :: MonadIO m => R.DB -> R.ColumnFamily -> m LogID
generateLogId db cf =
  do
    oldId <- R.getCF db def cf maxLogIdKey
    case oldId of
      Nothing -> do
        R.putCF db def cf maxLogIdKey (encode (1 :: Word64))
        return 1
      Just oid -> updateLogId oid
  where
    maxLogIdKey = "maxLogId"
    updateLogId oldId =
      do
        R.putCF db def cf maxLogIdKey (encode newLogId)
        return newLogId
      where
        newLogId :: Word64
        newLogId = deserialize oldId + 1

-- | generate entry Id
-- |
generateEntryId :: MonadIO m => IORef EntryID -> m EntryID
generateEntryId entryIdRef = liftIO $
  do
    oldId <- readIORef entryIdRef
    let newId = oldId + 1
    writeIORef entryIdRef newId
    return newId
