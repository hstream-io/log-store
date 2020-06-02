{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Log.Store.Base
  ( -- * Exported Types
    LogName,
    OpenOptions (..),
    LogHandle,
    Entry,
    EntryID,
    Env (..),
    Config (..),
    Context,

    -- * Basic functions
    initialize,
    open,
    appendEntry,
    readEntries,
    close,
    shutDown,
    withLogStore,

    -- * Options
    defaultOpenOptions,

    -- * utils
    serialize,
    deserialize,
  )
where

import Conduit ((.|), Conduit, ConduitT, mapC)
import Control.Exception (bracket)
import Control.Monad (mzero, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ReaderT, ask, runReaderT)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe (MaybeT (..))
import Control.Monad.Trans.Reader (mapReaderT)
import Control.Monad.Trans.Resource (MonadResource, MonadUnliftIO, ResourceT, allocate, runResourceT)
import Data.Binary (Binary)
import qualified Data.Binary as Binary
import Data.ByteString (ByteString, append)
import qualified Data.ByteString.Lazy as BSL
import Data.Default (def)
import Data.Word
import qualified Database.RocksDB as R
import System.FilePath.Posix ((</>))

-- | Log name
type LogName = String

-- | Log Id
type LogID = Word64

-- | LogHandle
data LogHandle
  = LogHandle
      { logName :: LogName,
        logID :: LogID,
        openOptions :: OpenOptions,
        metaDb :: R.DB,
        dataDb :: R.DB
      }

-- | entry which will be appended to a log
type Entry = ByteString

-- | entry Id
type EntryID = Word64

-- | open options
data OpenOptions
  = OpenOptions
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

-- | Config info
newtype Config = Config {rootDbPath :: FilePath}

dataDbDir :: FilePath
dataDbDir = "data"

metaDbDir :: FilePath
metaDbDir = "meta"

dataDbKey :: String
dataDbKey = "data"

metaDbKey :: String
metaDbKey = "meta"

dataDbPath :: Config -> FilePath
dataDbPath Config {..} = rootDbPath </> dataDbDir

metaDbPath :: Config -> FilePath
metaDbPath Config {..} = rootDbPath </> metaDbDir

-- | init Config
-- |
initConfig :: MonadIO m => Env -> m Config
initConfig DefaultEnv = return defaultConfig
initConfig (UserDefinedEnv config) = return config

-- | init
--
-- | 1. init config
-- | 2. create db path if necessary
-- | 3. return db handles
-- |
initialize :: MonadIO m => Env -> m Context
initialize env = liftIO $ do
  cfg@Config {..} <- initConfig env
  dataDb <-
    R.open
      (rootDbPath </> dataDbDir)
      R.defaultOptions
        { R.createIfMissing = True
        }
  metaDb <-
    R.open
      (rootDbPath </> metaDbDir)
      R.defaultOptions
        { R.createIfMissing = True,
          R.mergeOperator = R.UInt64Add
        }
  return Context {metaDbHandle = metaDb, dataDbHandle = dataDb}

data Env
  = DefaultEnv
  | UserDefinedEnv Config

-- | init Config from config file
defaultConfig :: Config
defaultConfig =
  Config
    { rootDbPath = "/usr/local/hstream/log-store"
    }

-- | it is used to generate a new logId while
-- | creating a new log.
-- |
-- | todo:
-- | exception need to consider
generateLogId :: MonadIO m => R.DB -> MaybeT m LogID
generateLogId metaDb =
  do
    oldId <- R.get metaDb def maxLogIdKey
    case oldId of
      Nothing -> do
        R.put metaDb def maxLogIdKey (serialize (1 :: Word64))
        return 1
      Just oid -> updateLogId oid
  where
    maxLogIdKey = "maxLogId"
    updateLogId oldId =
      do
        R.put metaDb def maxLogIdKey (serialize newLogId)
        return newLogId
      where
        newLogId :: Word64
        newLogId = (deserialize oldId) + 1

-- | serialize logId/entryId
serialize :: Binary b => b -> ByteString
serialize = BSL.toStrict . Binary.encode

-- | deserialize logId/entryId
deserialize :: Binary b => ByteString -> b
deserialize = Binary.decode . BSL.fromStrict

data Context
  = Context
      { metaDbHandle :: R.DB,
        dataDbHandle :: R.DB
      }

-- | open a log, will return a LogHandle for later operation
-- | (such as append and read)
open :: MonadIO m => LogName -> OpenOptions -> ReaderT Context (MaybeT m) LogHandle
open name op@OpenOptions {..} = do
  Context {metaDbHandle = mh, dataDbHandle = dh} <- ask
  logId <- R.get mh def (serialize name)
  case logId of
    Nothing ->
      if createIfMissing
        then lift $ do
          id <- generateLogId mh
          R.put mh def (serialize name) (serialize id)
          -- init entryId to 0
          R.put mh def (serialize id) (serialize (0 :: Word64))
          return $
            LogHandle
              { logName = name,
                logID = id,
                openOptions = op,
                dataDb = dh,
                metaDb = mh
              }
        else lift $ MaybeT $ return Nothing
    Just id ->
      return $
        LogHandle
          { logName = name,
            logID = deserialize id,
            openOptions = op,
            dataDb = dh,
            metaDb = mh
          }

-- | append an entry to log
appendEntry :: MonadIO m => LogHandle -> Entry -> ReaderT Context (MaybeT m) EntryID
appendEntry lh@LogHandle {..} entry =
  lift $
    if writeMode openOptions
      then do
        entryId <- generateEntryId metaDb logID
        saveEntry entryId
      else mzero
  where
    saveEntry id = do
      R.put dataDb def (generateKey logID id) entry
      return id

-- | generate key used to append entry
generateKey :: LogID -> EntryID -> ByteString
generateKey logId entryId = append (serialize logId) (serialize entryId)

-- | generate entry Id
-- |
generateEntryId :: MonadIO m => R.DB -> LogID -> MaybeT m EntryID
generateEntryId metaDb logId =
  do
    oldId <- MaybeT (R.get metaDb def maxEntryIdKey)
    updateEntryId oldId
  where
    maxEntryIdKey = serialize logId
    updateEntryId oldId =
      do
        R.put metaDb def maxEntryIdKey (serialize newEntryId)
        return newEntryId
      where
        newEntryId :: Word64
        newEntryId = (deserialize oldId) + 1

-- | read entries whose entryId in [firstEntry, LastEntry]
readEntries ::
  MonadIO m =>
  LogHandle ->
  EntryID ->
  EntryID ->
  ReaderT Context (MaybeT m) (ConduitT () (Maybe Entry) IO ())
readEntries LogHandle {..} firstKey lastKey = lift $ do
  source <- MaybeT (R.range dataDb first last)
  return (source .| mapC (fmap snd))
  where
    first = generateKey logID firstKey
    last = generateKey logID lastKey

-- | close log
-- |
-- | todo:
-- | what should do when call close?
-- | 1. free resource
-- | 2. once close, should forbid operation on this LogHandle
close :: MonadIO m => LogHandle -> ReaderT Context m ()
close LogHandle {..} = return ()

-- | free init resource
-- |
shutDown :: MonadIO m => ReaderT Context m ()
shutDown = do
  Context {..} <- ask
  R.close metaDbHandle
  R.close dataDbHandle

-- | function that wrap initialize and resource release.
-- | It is recommended to make open/append/read operations through this function.
-- |
withLogStore :: MonadUnliftIO m => Config -> ReaderT Context (MaybeT m) a -> m (Maybe a)
withLogStore cfg r =
  runResourceT
    ( do
        (_, ctx) <-
          allocate
            (initialize $ UserDefinedEnv cfg)
            (runReaderT shutDown)
        runReaderT (mapReaderT (lift . runMaybeT) r) ctx
    )
