{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Log.Store.Base
  ( -- * Exported Types
    LogName,
    Entry,
    OpenOptions (..),
    LogHandle,
    EntryID,
    Config (..),
    Context,
    LogStoreException (..),

    -- * Basic functions
    initialize,
    create,
    open,
    appendEntry,
    appendEntries,
    readEntries,
    close,
    shutDown,
    withLogStore,

    -- * Options
    defaultOpenOptions,
  )
where

import Control.Exception (throwIO)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ReaderT, ask, runReaderT)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Resource (MonadUnliftIO, allocate, runResourceT)
import Data.Bifunctor (first)
import qualified Data.ByteString as B
import qualified Data.ByteString.UTF8 as U
import Data.Default (def)
import Data.Function ((&))
import qualified Data.HashMap.Strict as H
import Data.Hashable (Hashable)
import Data.IORef (IORef, newIORef)
import Data.Maybe (isJust)
import qualified Data.Text as T
import Data.Vector (Vector, forM)
import Data.Word (Word32, Word64)
import qualified Database.RocksDB as R
import GHC.Conc (TVar, atomically, newTVarIO, readTVar, writeTVar)
import GHC.Generics (Generic)
import Log.Store.Exception
import Log.Store.Internal
import Log.Store.Utils
import Streamly (Serial)
import qualified Streamly.Prelude as S

-- | Config info
data Config = Config
  { rootDbPath :: FilePath,
    dataCfWriteBufferSize :: Word64,
    dbWriteBufferSize :: Word64,
    enableDBStatistics :: Bool,
    dbStatsDumpPeriodSec :: Word32
  }

data Context = Context
  { dbHandle :: R.DB,
    defaultCFHandle :: R.ColumnFamily,
    metaCFHandle :: R.ColumnFamily,
    dataCFHandle :: R.ColumnFamily,
    logHandleCache :: TVar (H.HashMap LogHandleKey LogHandle),
    maxLogIdRef :: IORef LogID
  }

-- | init Context using Config
initialize :: MonadIO m => Config -> m Context
initialize Config {..} =
  liftIO $ do
    (db, [defaultCF, metaCF, dataCF]) <-
      R.openColumnFamilies
        R.defaultDBOptions
          { R.createIfMissing = True,
            R.createMissingColumnFamilies = True,
            R.dbWriteBufferSize = dbWriteBufferSize,
            R.maxBackgroundCompactions = 1,
            R.maxBackgroundFlushes = 1,
            R.enableStatistics = enableDBStatistics,
            R.statsDumpPeriodSec = dbStatsDumpPeriodSec
          }
        rootDbPath
        [ R.ColumnFamilyDescriptor
            { name = defaultCFName,
              options = R.defaultDBOptions
            },
          R.ColumnFamilyDescriptor
            { name = metaCFName,
              options = R.defaultDBOptions
            },
          R.ColumnFamilyDescriptor
            { name = dataCFName,
              options =
                R.defaultDBOptions
                  { R.writeBufferSize = dataCfWriteBufferSize,
                    R.disableAutoCompactions = True,
                    R.level0FileNumCompactionTrigger = -1,
                    R.level0SlowdownWritesTrigger = -1,
                    R.level0StopWritesTrigger = -1,
                    R.softPendingCompactionBytesLimit = 18446744073709551615,
                    R.hardPendingCompactionBytesLimit = 18446744073709551615
                  }
            }
        ]
    cache <- newTVarIO H.empty
    maxLogId <- getMaxLogId db metaCF
    logIdRef <- newIORef maxLogId
    return
      Context
        { dbHandle = db,
          defaultCFHandle = defaultCF,
          metaCFHandle = metaCF,
          dataCFHandle = dataCF,
          logHandleCache = cache,
          maxLogIdRef = logIdRef
        }
  where
    dataCFName = "data"
    metaCFName = "meta"
    defaultCFName = "default"

    getMaxLogId :: R.DB -> R.ColumnFamily -> IO LogID
    getMaxLogId db cf = do
      maxLogId <- R.getCF db def cf maxLogIdKey
      case maxLogId of
        Nothing -> do
          let initLogId = 0
          R.putCF db def cf maxLogIdKey $ encodeWord64 initLogId
          return initLogId
        Just v -> return (decodeWord64 v)

-- | open options
data OpenOptions = OpenOptions
  { readMode :: Bool,
    writeMode :: Bool,
    createIfMissing :: Bool
  }
  deriving (Eq, Show, Generic)

instance Hashable OpenOptions

defaultOpenOptions =
  OpenOptions
    { readMode = True,
      writeMode = False,
      createIfMissing = False
    }

type Entry = B.ByteString

-- | LogHandle
data LogHandle = LogHandle
  { logName :: LogName,
    logID :: LogID,
    openOptions :: OpenOptions,
    maxEntryIdRef :: IORef EntryID
  }
  deriving (Eq)

data LogHandleKey
  = LogHandleKey LogName OpenOptions
  deriving (Eq, Show, Generic)

instance Hashable LogHandleKey

-- | open a log, will return a LogHandle for later operation
-- | (such as append and read)
open :: MonadIO m => LogName -> OpenOptions -> ReaderT Context m LogHandle
open name opts@OpenOptions {..} = do
  Context {..} <- ask
  res <- R.getCF dbHandle def metaCFHandle (encodeText name)
  let valid = checkOpts res
  if valid
    then do
      cacheRes <- lookupCache logHandleCache
      case cacheRes of
        Just lh ->
          return lh
        Nothing -> do
          newLh <- mkLogHandle res
          updateCache logHandleCache newLh
    else
      liftIO $
        throwIO $
          LogStoreLogNotFoundException $
            "no log named " ++ T.unpack name ++ " found"
  where
    logHandleKey = LogHandleKey name opts

    lookupCache :: MonadIO m => TVar (H.HashMap LogHandleKey LogHandle) -> m (Maybe LogHandle)
    lookupCache tc = liftIO $
      atomically $ do
        cache <- readTVar tc
        case H.lookup logHandleKey cache of
          Nothing -> return Nothing
          Just lh -> return $ Just lh

    updateCache :: MonadIO m => TVar (H.HashMap LogHandleKey LogHandle) -> LogHandle -> m LogHandle
    updateCache tc lh = liftIO $
      atomically $ do
        cache <- readTVar tc
        case H.lookup logHandleKey cache of
          Nothing -> do
            let newCache = H.insert logHandleKey lh cache
            writeTVar tc newCache
            return lh
          Just handle -> return handle

    mkLogHandle :: MonadIO m => Maybe B.ByteString -> ReaderT Context m LogHandle
    mkLogHandle res =
      case res of
        Nothing -> do
          id <- create name
          maxEntryIdRef <- liftIO $ newIORef minEntryId
          return $
            LogHandle
              { logName = name,
                logID = id,
                openOptions = opts,
                maxEntryIdRef = maxEntryIdRef
              }
        Just bId ->
          do
            let logId = decodeLogId bId
            maxEntryId <- getMaxEntryId logId
            maxEntryIdRef <- liftIO $ newIORef maxEntryId
            return $
              LogHandle
                { logName = name,
                  logID = logId,
                  openOptions = opts,
                  maxEntryIdRef = maxEntryIdRef
                }

    checkOpts :: Maybe B.ByteString -> Bool
    checkOpts res =
      case res of
        Nothing ->
          createIfMissing
        Just _ -> True

getMaxEntryId :: MonadIO m => LogID -> ReaderT Context m EntryID
getMaxEntryId logId = do
  Context {..} <- ask
  R.withIteratorCF dbHandle def dataCFHandle findMaxEntryId
  where
    findMaxEntryId iterator = do
      R.seekForPrev iterator (encodeEntryKey $ EntryKey logId maxEntryId)
      isValid <- R.valid iterator
      if isValid
        then do
          entryKey <- R.key iterator
          let (EntryKey _ entryId) = decodeEntryKey entryKey
          return entryId
        else do
          errStr <- R.getError iterator
          case errStr of
            Nothing -> throwIO $ LogStoreIOException "getMaxEntryId occurs error"
            Just str -> throwIO $ LogStoreIOException $ "getMaxEntryId error: " ++ str

exists :: MonadIO m => LogName -> ReaderT Context m Bool
exists name = do
  Context {..} <- ask
  logId <- R.getCF dbHandle def metaCFHandle (encodeLogName name)
  return $ isJust logId

create :: MonadIO m => LogName -> ReaderT Context m LogID
create name = do
  flag <- exists name
  if flag
    then
      liftIO $
        throwIO $
          LogStoreLogAlreadyExistsException $ "log " ++ T.unpack name ++ " already existed"
    else do
      Context {..} <- ask
      R.withWriteBatch $ initLog dbHandle metaCFHandle dataCFHandle maxLogIdRef
  where
    initLog db metaCf dataCf maxLogIdRef batch = do
      logId <- generateLogId db metaCf maxLogIdRef
      R.batchPutCF batch metaCf (encodeLogName name) (encodeLogId logId)
      R.batchPutCF
        batch
        dataCf
        (encodeEntryKey $ EntryKey logId minEntryId)
        (U.fromString "first entry")
      R.write db def batch
      return logId

-- | append an entry to log
appendEntry :: MonadIO m => LogHandle -> Entry -> ReaderT Context m EntryID
appendEntry LogHandle {..} entry = do
  Context {..} <- ask
  if writeMode openOptions
    then do
      entryId <- generateEntryId maxEntryIdRef
      R.putCF
        dbHandle
        R.defaultWriteOptions {R.disableWAL = True}
        dataCFHandle
        (encodeEntryKey $ EntryKey logID entryId)
        entry
      return entryId
    else
      liftIO $
        throwIO $
          LogStoreUnsupportedOperationException $ "log named " ++ T.unpack logName ++ " is not writable."

appendEntries :: MonadIO m => LogHandle -> Vector Entry -> ReaderT Context m (Vector EntryID)
appendEntries LogHandle {..} entries = do
  Context {..} <- ask
  R.withWriteBatch $ appendEntries' dbHandle dataCFHandle
  where
    appendEntries' db cf batch = do
      entryIds <-
        forM
          entries
          batchAdd
      R.write db def batch
      return entryIds
      where
        batchAdd entry = do
          entryId <- generateEntryId maxEntryIdRef
          R.batchPutCF batch cf (encodeEntryKey $ EntryKey logID entryId) entry
          return entryId

readEntries ::
  MonadIO m =>
  LogHandle ->
  Maybe EntryID ->
  Maybe EntryID ->
  ReaderT Context m (Serial (Entry, EntryID))
readEntries LogHandle {..} firstKey lastKey = do
  Context {..} <- ask
  let kvStream = R.rangeCF dbHandle def dataCFHandle start end
  return $
    kvStream
      & S.map (first decodeEntryKey)
      -- & S.filter (\(EntryKey logId _, _) -> logId == logID)
      & S.map (\(EntryKey _ entryId, entry) -> (entry, entryId))
  where
    start =
      case firstKey of
        Nothing -> Just $ encodeEntryKey $ EntryKey logID firstNormalEntryId
        Just k -> Just $ encodeEntryKey $ EntryKey logID k
    end =
      case lastKey of
        Nothing -> Just $ encodeEntryKey $ EntryKey logID maxEntryId
        Just k -> Just $ encodeEntryKey $ EntryKey logID k

-- | close log
-- |
-- | todo:
-- | what should do when call close?
-- | 1. free resource
-- | 2. once close, should forbid operation on this LogHandle
close :: MonadIO m => LogHandle -> ReaderT Context m ()
close LogHandle {..} = return ()

-- | shutDown and free resources
shutDown :: MonadIO m => ReaderT Context m ()
shutDown = do
  Context {..} <- ask
  R.destroyColumnFamily defaultCFHandle
  R.destroyColumnFamily metaCFHandle
  R.destroyColumnFamily dataCFHandle
  R.close dbHandle

-- | function that wrap initialize and resource release.
withLogStore :: MonadUnliftIO m => Config -> ReaderT Context m a -> m a
withLogStore cfg r =
  runResourceT
    ( do
        (_, ctx) <-
          allocate
            (initialize cfg)
            (runReaderT shutDown)
        lift $ runReaderT r ctx
    )
