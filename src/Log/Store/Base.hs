{-# LANGUAGE RecordWildCards #-}

module Log.Store.Base
  ( -- * Exported Types
    LogName,
    Entry,
    OpenOptions (..),
    LogHandle (..),
    EntryID,
    Config (..),
    Context,

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
import qualified Data.ByteString as B
import qualified Data.ByteString.UTF8 as U
import Data.Default (def)
import Data.Function ((&))
import Data.IORef (IORef, newIORef)
import Data.Maybe (isJust)
import qualified Data.Text as T
import Data.Vector (Vector, forM)
import qualified Database.RocksDB as R
import Log.Store.Exception
import Log.Store.Internal
import Log.Store.Utils
import Streamly (Serial)
import qualified Streamly.Prelude as S

-- | Config info
newtype Config = Config {rootDbPath :: FilePath}

data Context = Context
  { dbHandle :: R.DB,
    defaultCFHandle :: R.ColumnFamily,
    metaCFHandle :: R.ColumnFamily,
    dataCFHandle :: R.ColumnFamily
  }

-- | init Context using Config
initialize :: MonadIO m => Config -> m Context
initialize Config {..} =
  liftIO $ do
    (db, [defaultCF, metaCF, dataCF]) <-
      R.openColumnFamilies
        R.defaultDBOptions
          { R.createIfMissing = True,
            R.createMissingColumnFamilies = True
          }
        rootDbPath
        [ R.ColumnFamilyDescriptor {name = defaultCFName, options = R.defaultDBOptions},
          R.ColumnFamilyDescriptor {name = metaCFName, options = R.defaultDBOptions},
          R.ColumnFamilyDescriptor {name = dataCFName, options = R.defaultDBOptions}
        ]
    return
      Context
        { dbHandle = db,
          defaultCFHandle = defaultCF,
          metaCFHandle = metaCF,
          dataCFHandle = dataCF
        }
  where
    dataCFName = "data"
    metaCFName = "meta"
    defaultCFName = "default"

-- | open options
data OpenOptions = OpenOptions
  { readMode :: Bool,
    writeMode :: Bool,
    createIfMissing :: Bool
  }

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

-- | open a log, will return a LogHandle for later operation
-- | (such as append and read)
open :: MonadIO m => LogName -> OpenOptions -> ReaderT Context m LogHandle
open name op@OpenOptions {..} = do
  Context {..} <- ask
  logId <- R.getCF dbHandle def metaCFHandle (encodeText name)
  case logId of
    Nothing ->
      if createIfMissing
        then do
          id <- create name
          maxEntryIdRef <- liftIO $ newIORef minEntryId
          return $
            LogHandle
              { logName = name,
                logID = id,
                openOptions = op,
                maxEntryIdRef = maxEntryIdRef
              }
        else
          liftIO $
            throwIO $
              LogStoreLogNotFoundException $ "no log named " ++ T.unpack name ++ " found"
    Just id ->
      do
        let logId = decodeLogId id
        maxEntryId <- getMaxEntryId logId
        maxEntryIdRef <- liftIO $ newIORef maxEntryId
        return $
          LogHandle
            { logName = name,
              logID = logId,
              openOptions = op,
              maxEntryIdRef = maxEntryIdRef
            }

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
      R.withWriteBatch $ initLog dbHandle metaCFHandle dataCFHandle
  where
    initLog db metaCf dataCf batch = do
      logId <- generateLogId db metaCf
      R.batchPutCF batch metaCf (encodeLogName name) (encodeLogId logId)
      R.batchPutCF
        batch
        dataCf
        (encodeEntryKey $ EntryKey logId minEntryId)
        (encodeInnerEntry $ InnerEntry minEntryId $ U.fromString "first entry")
      R.write db def batch
      return logId

-- | append an entry to log
appendEntry :: MonadIO m => LogHandle -> Entry -> ReaderT Context m EntryID
appendEntry LogHandle {..} entry = do
  Context {..} <- ask
  if writeMode openOptions
    then do
      entryId <- generateEntryId maxEntryIdRef
      let valueBstr = encodeInnerEntry $ InnerEntry entryId entry
      R.putCF dbHandle def dataCFHandle (encodeEntryKey $ EntryKey logID entryId) valueBstr
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
          R.batchPutCF batch cf (encodeEntryKey $ EntryKey logID entryId) (encodeInnerEntry $ InnerEntry entryId entry)
          return entryId

-- | read entries whose entryId in [firstEntry, LastEntry]
readEntries ::
  MonadIO m =>
  LogHandle ->
  Maybe EntryID ->
  Maybe EntryID ->
  ReaderT Context m (Serial (Entry, EntryID))
readEntries LogHandle {..} firstKey lastKey = do
  Context {..} <- ask
  let kvStream = R.rangeCF dbHandle def dataCFHandle first last
  return $
    kvStream
      & S.map snd
      & S.map decodeInnerEntry
      & S.map (\(InnerEntry entryId entry) -> (entry, entryId))
  where
    first =
      case firstKey of
        Nothing -> Just $ encodeEntryKey $ EntryKey logID firstNormalEntryId
        Just k -> Just $ encodeEntryKey $ EntryKey logID k
    last =
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
