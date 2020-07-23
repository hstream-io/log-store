{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE OverloadedStrings #-}

module Log.Store.Internal where

import ByteString.StrictBuilder (builderBytes, bytes, word64BE)
import Control.Exception (throw)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Atomics (atomicModifyIORefCAS)
import Data.Binary.Strict.Get as BinStrict
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

-- | entry content with some meta info
data InnerEntry = InnerEntry EntryID B.ByteString
  deriving (Eq, Show)

encodeInnerEntry :: InnerEntry -> B.ByteString
encodeInnerEntry (InnerEntry entryId content) =
  builderBytes $ word64BE entryId `mappend` bytes content

decodeInnerEntry :: B.ByteString -> InnerEntry
decodeInnerEntry bs =
  if rem /= B.empty
    then throw $ LogStoreDecodeException "input error"
    else case res of
      Left s -> throw $ LogStoreDecodeException s
      Right v -> v
  where
    (res, rem) = decode' bs
    decode' = runGet $ do
      entryId <- getWord64be
      len <- remaining
      content <- getByteString len
      return $ InnerEntry entryId content

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
-- |
-- | todo:
-- | exception need to consider
generateLogId :: MonadIO m => R.DB -> R.ColumnFamily -> m LogID
generateLogId db cf =
  do
    oldId <- R.getCF db def cf maxLogIdKey
    case oldId of
      Nothing -> do
        R.putCF db def cf maxLogIdKey (encodeWord64 1)
        return 1
      Just oid -> updateLogId oid
  where
    maxLogIdKey = "maxLogId"
    updateLogId oldId =
      do
        R.putCF db def cf maxLogIdKey (encodeWord64 newLogId)
        return newLogId
      where
        newLogId :: Word64
        newLogId = decodeWord64 oldId + 1

-- | generate entry Id
-- |
generateEntryId :: MonadIO m => IORef EntryID -> m EntryID
generateEntryId entryIdRef =
  liftIO $
    atomicModifyIORefCAS entryIdRef (\curId -> (curId + 1, curId + 1))
