{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE OverloadedStrings #-}

module Log.Store.Internal where

import ByteString.StrictBuilder (builderBytes, word64BE)
import Control.Exception (throw)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Atomics (atomicModifyIORefCAS)
import Data.Binary.Strict.Get (getWord64be, runGet)
import Data.ByteString as B
import Data.Default (def)
import Data.IORef (IORef)
import Data.Text as T
import Data.Word (Word64)
import qualified Database.RocksDB as R
import Log.Store.Exception
import Log.Store.Utils

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

firstNormalEntryId :: EntryID
firstNormalEntryId = 1

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
generateLogId :: MonadIO m => R.DB -> R.ColumnFamily -> IORef LogID -> m LogID
generateLogId db cf logIdRef =
  liftIO $ do
    newId <- atomicModifyIORefCAS logIdRef (\curId -> (curId + 1, curId + 1))
    R.putCF db def cf maxLogIdKey (encodeWord64 newId)
    return newId

-- | generate entry Id
-- |
generateEntryId :: MonadIO m => IORef EntryID -> m EntryID
generateEntryId entryIdRef =
  liftIO $
    atomicModifyIORefCAS entryIdRef (\curId -> (curId + 1, curId + 1))
