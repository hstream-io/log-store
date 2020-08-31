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
    open,
    appendEntry,
    appendEntries,
    readEntries,
    close,
    shutDown,
    withLogStore,

    -- * Options
    defaultOpenOptions,
    defaultConfig,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async, async, cancel)
import qualified Control.Concurrent.ReadWriteLock as RWL
import Control.Concurrent.STM (TVar, atomically, newTVarIO, readTVar, retry, writeTVar)
import Control.Exception (bracket, throwIO)
import Control.Monad (foldM, forever, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ReaderT, ask, runReaderT)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Resource (MonadUnliftIO, allocate, runResourceT)
import qualified Data.ByteString as B
import qualified Data.Cache.LRU as L
import Data.Default (def)
import qualified Data.HashMap.Strict as H
import Data.Hashable (Hashable)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe, isJust)
import Data.Sequence (Seq (..), (><), (|>))
import qualified Data.Sequence as Seq
import qualified Data.Text as T
import qualified Data.Vector as V
import Data.Word (Word32, Word64)
import qualified Database.RocksDB as R
import GHC.Generics (Generic)
import Log.Store.Exception
import Log.Store.Internal
import Log.Store.Utils
import System.Directory (createDirectoryIfMissing)
import System.FilePath ((</>))

-- | Config info
data Config = Config
  { rootDbPath :: FilePath,
    dataCfWriteBufferSize :: Word64,
    enableDBStatistics :: Bool,
    dbStatsDumpPeriodSec :: Word32,
    partitionInterval :: Int, -- seconds
    partitionFilesNumLimit :: Int,
    maxOpenDbs :: Int
  }

defaultConfig :: Config
defaultConfig =
  Config
    { rootDbPath = "/tmp/log-store/rocksdb",
      dataCfWriteBufferSize = 200 * 1024 * 1024,
      enableDBStatistics = False,
      dbStatsDumpPeriodSec = 600,
      partitionInterval = 60,
      partitionFilesNumLimit = 16,
      maxOpenDbs = -1
    }

data Context = Context
  { dbPath :: FilePath,
    metaDbHandle :: R.DB,
    rwLockForCurDb :: RWL.RWLock,
    curDataDbHandleRef :: IORef R.DB,
    logHandleCache :: TVar (H.HashMap LogHandleKey LogHandle),
    maxLogIdRef :: IORef LogID,
    backgroundShardingTask :: Async (),
    dbHandlesForReadCache :: TVar (L.LRU String R.DB),
    dbHandlesForReadRcMap :: TVar (H.HashMap String Int),
    dbHandlesForReadEvcited :: TVar (H.HashMap String R.DB)
  }

shardingTask ::
  Int ->
  Int ->
  FilePath ->
  Word64 ->
  RWL.RWLock ->
  IORef R.DB ->
  IO ()
shardingTask
  partitionInterval
  partitionFilesNumLimit
  dbPath
  cfWriteBufferSize
  rwLock
  curDataDbHandleRef = forever $ do
    threadDelay $ partitionInterval * 1000000
    curDb <- RWL.withRead rwLock $ readIORef curDataDbHandleRef
    curDataDbFilesNum <- getFilesNumInDb curDb
    when
      (curDataDbFilesNum >= partitionFilesNumLimit)
      $ RWL.withWrite
        rwLock
        ( do
            newDbName <- generateDataDbName
            newDbHandle <- createDataDb dbPath newDbName cfWriteBufferSize
            R.flush curDb def
            R.close curDb
            writeIORef curDataDbHandleRef newDbHandle
        )

openMetaDb :: MonadIO m => Config -> m R.DB
openMetaDb Config {..} = do
  liftIO $ createDirectoryIfMissing True rootDbPath
  R.open
    R.defaultDBOptions
      { R.createIfMissing = True
      }
    (rootDbPath </> metaDbName)

-- | init Context using Config
-- 1. open (or create) db, metaCF, create a new dataCF
-- 2. init context variables: logHandleCache, maxLogIdRef
-- 3. start background task: shardingTask
initialize :: MonadIO m => Config -> m Context
initialize cfg@Config {..} =
  liftIO $ do
    metaDb <- openMetaDb cfg

    newDataDbName <- generateDataDbName
    newDataDbHandle <- createDataDb rootDbPath newDataDbName dataCfWriteBufferSize
    newDataDbHandleRef <- newIORef newDataDbHandle

    newRWLock <- RWL.new
    logHandleCache <- newTVarIO H.empty
    maxLogId <- getMaxLogId metaDb
    logIdRef <- newIORef maxLogId
    bgShardingTask <-
      async $
        shardingTask
          partitionInterval
          partitionFilesNumLimit
          rootDbPath
          dataCfWriteBufferSize
          newRWLock
          newDataDbHandleRef

    let cacheSize = if maxOpenDbs <= 0 then Nothing else Just $ toInteger maxOpenDbs
    dbHandlesForReadCache <- newTVarIO $ L.newLRU cacheSize
    dbHandlesForReadRcMap <- newTVarIO H.empty
    dbHandlesForReadEvcited <- newTVarIO H.empty

    return
      Context
        { dbPath = rootDbPath,
          metaDbHandle = metaDb,
          rwLockForCurDb = newRWLock,
          curDataDbHandleRef = newDataDbHandleRef,
          logHandleCache = logHandleCache,
          maxLogIdRef = logIdRef,
          backgroundShardingTask = bgShardingTask,
          dbHandlesForReadCache = dbHandlesForReadCache,
          dbHandlesForReadRcMap = dbHandlesForReadRcMap,
          dbHandlesForReadEvcited = dbHandlesForReadEvcited
        }
  where
    getMaxLogId :: R.DB -> IO LogID
    getMaxLogId db = do
      maxLogId <- R.get db def maxLogIdKey
      case maxLogId of
        Nothing -> do
          let initLogId = 0
          R.put db def maxLogIdKey $ encodeWord64 initLogId
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
  res <- R.get metaDbHandle def (encodeText name)
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
            res <- getMaxEntryId logId
            maxEntryIdRef <- liftIO $ newIORef $ fromMaybe minEntryId res
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

-- getDataCfHandlesForRead :: FilePath -> IO (R.DB, [R.ColumnFamily])
-- getDataCfHandlesForRead dbPath = do
--   cfNames <- R.listColumnFamilies def dbPath
--   let dataCfNames = filter (/= metaCFName) cfNames
--   let dataCfDescriptors = map (\cfName -> R.ColumnFamilyDescriptor {name = cfName, options = def}) dataCfNames
--   (dbForReadOnly, handles) <-
--     R.openForReadOnlyColumnFamilies
--       def
--       dbPath
--       dataCfDescriptors
--       False
--   return (dbForReadOnly, tail handles)
--

withDbHandleForRead ::
  TVar (L.LRU String R.DB) ->
  TVar (H.HashMap String R.DB) ->
  TVar (H.HashMap String Int) ->
  FilePath ->
  String ->
  (R.DB -> IO a) ->
  IO a
withDbHandleForRead
  dbHandleCache
  dbHandlesEvicted
  dbHandleRcMap
  dbPath
  dbName =
    bracket
      ( do
          r <-
            atomically
              ( do
                  cache <- readTVar dbHandleCache
                  let (newCache, res) = L.lookup dbName cache
                  case res of
                    Nothing -> do
                      rcMap <- readTVar dbHandleRcMap
                      let rc = H.lookupDefault 0 dbName rcMap
                      let newRcMap = H.insert dbName (rc + 1) rcMap
                      writeTVar dbHandleRcMap newRcMap
                      if rc == 0
                        then return Nothing
                        else retry
                    Just v -> do
                      writeTVar dbHandleCache newCache
                      return $ Just v
              )

          case r of
            Nothing -> do
              dbHandle <-
                R.openForReadOnly
                  def
                  (dbPath </> dbName)
                  False
              insertDbHandleToCache dbHandle
              return dbHandle
            Just handle ->
              return handle
      )
      ( \dbHandle -> do
          shouldClose <- unRef
          when shouldClose $
            R.close dbHandle
      )
    where
      insertDbHandleToCache :: R.DB -> IO ()
      insertDbHandleToCache dbHandle =
        atomically
          ( do
              cache <- readTVar dbHandleCache
              let (newCache, evictedKV) = L.insertInforming dbName dbHandle cache
              writeTVar dbHandleCache newCache
              case evictedKV of
                Nothing -> return ()
                Just (k, v) -> do
                  s <- readTVar dbHandlesEvicted
                  writeTVar dbHandlesEvicted $ H.insert k v s
          )

      unRef :: IO Bool
      unRef =
        atomically $
          do
            rcMap <- readTVar dbHandleRcMap
            let rc = H.lookup dbName rcMap
            case rc of
              Just count -> do
                writeTVar dbHandleRcMap $ H.adjust (+ (-1)) dbName rcMap
                if count == 0
                  then do
                    s <- readTVar dbHandlesEvicted
                    case H.lookup dbName s of
                      Nothing -> return False
                      Just _ -> do
                        writeTVar dbHandlesEvicted $ H.delete dbName s
                        return True
                  else return False
              Nothing -> error "this should never reach"

getMaxEntryId :: MonadIO m => LogID -> ReaderT Context m (Maybe EntryID)
getMaxEntryId logId = do
  Context {..} <- ask
  readOnlyDataDbNames <- getReadOnlyDataDbNames dbPath rwLockForCurDb
  foldM
    (f dbHandlesForReadCache dbHandlesForReadEvcited dbHandlesForReadRcMap dbPath)
    Nothing
    (reverse readOnlyDataDbNames)
  where
    f cache gcMap rcMap dbPath prevRes dbName =
      case prevRes of
        Nothing ->
          liftIO $
            withDbHandleForRead
              cache
              gcMap
              rcMap
              dbPath
              dbName
              (\dbForRead -> R.withIterator dbForRead def findMaxEntryIdInDb)
        Just res -> return $ Just res

    findMaxEntryIdInDb :: MonadIO m => R.Iterator -> m (Maybe EntryID)
    findMaxEntryIdInDb iterator = do
      R.seekForPrev iterator (encodeEntryKey $ EntryKey logId maxEntryId)
      isValid <- R.valid iterator
      if isValid
        then do
          entryKey <- R.key iterator
          let (EntryKey entryLogId entryId) = decodeEntryKey entryKey
          if entryLogId == logId
            then return $ Just entryId
            else return Nothing
        else do
          errStr <- R.getError iterator
          case errStr of
            Nothing -> return Nothing
            Just str -> liftIO $ throwIO $ LogStoreIOException $ "getMaxEntryId error: " ++ str

exists :: MonadIO m => LogName -> ReaderT Context m Bool
exists name = do
  Context {..} <- ask
  logId <- R.get metaDbHandle def (encodeLogName name)
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
      initLog metaDbHandle maxLogIdRef
  where
    initLog metaDb maxLogIdRef = do
      logId <- generateLogId metaDb maxLogIdRef
      R.put metaDb def (encodeLogName name) (encodeLogId logId)
      return logId

-- | append an entry to log
appendEntry :: MonadIO m => LogHandle -> Entry -> ReaderT Context m EntryID
appendEntry LogHandle {..} entry = do
  Context {..} <- ask
  if writeMode openOptions
    then
      liftIO $
        RWL.withRead
          rwLockForCurDb
          ( do
              entryId <- generateEntryId maxEntryIdRef
              curDataDb <- readIORef curDataDbHandleRef
              R.put
                curDataDb
                R.defaultWriteOptions
                (encodeEntryKey $ EntryKey logID entryId)
                entry
              return entryId
          )
    else
      liftIO $
        throwIO $
          LogStoreUnsupportedOperationException $ "log named " ++ T.unpack logName ++ " is not writable."

appendEntries :: MonadIO m => LogHandle -> V.Vector Entry -> ReaderT Context m (V.Vector EntryID)
appendEntries LogHandle {..} entries = do
  Context {..} <- ask
  liftIO $
    RWL.withRead
      rwLockForCurDb
      ( do
          curDataDb <- readIORef curDataDbHandleRef
          R.withWriteBatch $ appendEntries' curDataDb
      )
  where
    appendEntries' db batch = do
      entryIds <-
        V.forM
          entries
          batchAdd
      R.write db def batch
      return entryIds
      where
        batchAdd entry = do
          entryId <- generateEntryId maxEntryIdRef
          R.batchPut batch (encodeEntryKey $ EntryKey logID entryId) entry
          return entryId

-- readEntries ::
--   MonadIO m =>
--   LogHandle ->
--   Maybe EntryID ->
--   Maybe EntryID ->
--   ReaderT Context m (Serial (Entry, EntryID))
-- readEntries LogHandle {..} firstKey lastKey = do
--   Context {..} <- ask
--   dataCfNames <- getDataCfNameSet dbPath
--   streams <- mapM (readEntriesInCf resourcesForReadRef dbPath) dataCfNames
--   return $ sconcat $ fromList streams
--   where
--     readEntriesInCf resRef dbPath dataCfName =
--       liftIO $ do
--         cfr@CFResourcesForRead {..} <- openCFReadOnly dbPath dataCfName
--         atomicModifyIORefCAS resRef (\res -> (res ++ [cfr], res))
--         let kvStream = R.rangeCF dbHandleForRead def cfHandleForRead start end
--         return $
--           kvStream
--             & S.map (first decodeEntryKey)
--             -- & S.filter (\(EntryKey logId _, _) -> logId == logID)
--             & S.map (\(EntryKey _ entryId, entry) -> (entry, entryId))
--
--     start =
--       case firstKey of
--         Nothing -> Just $ encodeEntryKey $ EntryKey logID firstNormalEntryId
--         Just k -> Just $ encodeEntryKey $ EntryKey logID k
--     end =
--       case lastKey of
--         Nothing -> Just $ encodeEntryKey $ EntryKey logID maxEntryId
--         Just k -> Just $ encodeEntryKey $ EntryKey logID k

readEntries ::
  MonadIO m =>
  LogHandle ->
  Maybe EntryID ->
  Maybe EntryID ->
  ReaderT Context m (Seq (EntryID, Entry))
readEntries LogHandle {..} firstKey lastKey = do
  Context {..} <- ask
  readOnlyDataDbNames <- getReadOnlyDataDbNames dbPath rwLockForCurDb
  prevRes <-
    foldM
      (f dbHandlesForReadCache dbHandlesForReadEvcited dbHandlesForReadRcMap logID dbPath)
      Seq.empty
      readOnlyDataDbNames
  if goOn prevRes
    then
      liftIO $
        RWL.withRead
          rwLockForCurDb
          ( do
              curDb <- readIORef curDataDbHandleRef
              res <- R.withIterator curDb def $ readEntriesInDb logID
              return $ prevRes >< res
          )
    else return prevRes
  where
    -- f :: MonadIO m => LogID -> FilePath -> [(EntryID, Entry)] -> String -> m [(EntryID, Entry)]
    f cache gcMap rcMap logId dbPath prevRes dataDbName =
      if goOn prevRes
        then liftIO $ do
          res <-
            withDbHandleForRead
              cache
              gcMap
              rcMap
              dbPath
              dataDbName
              (\dbForRead -> R.withIterator dbForRead def $ readEntriesInDb logId)
          return $ prevRes >< res
        else return prevRes

    goOn prevRes = Seq.null prevRes || fst (lastElemInSeq prevRes) < limitEntryId

    lastElemInSeq seq =
      case seq of
        Seq.Empty -> error "empty sequence"
        _ :|> x -> x

    readEntriesInDb :: MonadIO m => LogID -> R.Iterator -> m (Seq (EntryID, Entry))
    readEntriesInDb logId iterator = do
      R.seek iterator (encodeEntryKey $ EntryKey logId startEntryId)
      loop logId iterator Seq.empty

    loop :: MonadIO m => LogID -> R.Iterator -> Seq (EntryID, Entry) -> m (Seq (EntryID, Entry))
    loop logId iterator acc = do
      isValid <- R.valid iterator
      if isValid
        then do
          entryKey <- R.key iterator
          let (EntryKey entryLogId entryId) = decodeEntryKey entryKey
          if entryLogId == logId && entryId <= limitEntryId
            then do
              entry <- R.value iterator
              R.next iterator
              loop logId iterator (acc |> (entryId, entry))
            else return acc
        else do
          errStr <- R.getError iterator
          case errStr of
            Nothing -> return acc
            Just str -> liftIO $ throwIO $ LogStoreIOException $ "readEntries error: " ++ str

    startEntryId = fromMaybe minEntryId firstKey
    limitEntryId = fromMaybe maxEntryId lastKey

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
  liftIO $ cancel backgroundShardingTask

  readOnlyDbHandles <- liftIO $ dbHandlesWaitForFree dbHandlesForReadCache dbHandlesForReadEvcited
  mapM_ R.close readOnlyDbHandles

  R.close metaDbHandle
  curDataDbHandle <- liftIO $ readIORef curDataDbHandleRef
  R.close curDataDbHandle
  where
    dbHandlesWaitForFree cache gcMap = atomically $ do
      c <- readTVar cache
      g <- readTVar gcMap
      return $ fmap snd $ L.toList c ++ H.toList g

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
