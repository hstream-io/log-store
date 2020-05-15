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

    -- * Basic functions
    initialize,
    open,
    appendEntry,
    readEntries,
    close,
    defaultOpenOptions,

    -- * utils
    serialize,
    deserialize,

    -- * internal temp for test

    -- * later will be moved in Internal Module
    generateLogId,
  )
where

import Conduit ((.|), Conduit, ConduitT, mapC)
import Control.Exception (bracket)
import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ReaderT, ask)
import Control.Monad.Trans.Resource (allocate, runResourceT)
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
        dataDb :: R.DB,
        metaDb :: R.DB
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
data Config
  = Config
      { -- | parent path for data and metadata
        rootDbPath :: FilePath
      }

dataDbDir :: FilePath
dataDbDir = "data"

metaDbDir :: FilePath
metaDbDir = "meta"

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
-- | 3. exec some check
-- |
initialize :: MonadIO m => Env -> m Config
initialize env = liftIO $ runResourceT $ do
  cfg@Config {..} <- initConfig env
  allocate
    ( R.open
        (rootDbPath </> dataDbDir)
        R.defaultOptions
          { R.createIfMissing = True
          }
    )
    R.close
  allocate
    ( R.open
        (rootDbPath </> metaDbDir)
        R.defaultOptions
          { R.createIfMissing = True
          }
    )
    R.close
  return cfg

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
generateLogId :: MonadIO m => R.DB -> m (Maybe LogID)
generateLogId metaDb =
  do
    R.merge metaDb def maxLogIdKey (serialize (1 :: Word64))
    newId <- R.get metaDb def maxLogIdKey
    case newId of
      Nothing -> return Nothing
      Just nid -> return $ Just $ deserialize nid
  where
    maxLogIdKey = "maxLogId"

-- | serialize logId/entryId
serialize :: Binary b => b -> ByteString
serialize = BSL.toStrict . Binary.encode

-- | deserialize logId/entryId
deserialize :: Binary b => ByteString -> b
deserialize = Binary.decode . BSL.fromStrict

-- | open a logic log by name.
-- | note that at last db pointer in LogHandle should be release
-- |
open :: MonadIO m => LogName -> OpenOptions -> ReaderT Config m (Maybe LogHandle)
open name op@OpenOptions {..} = do
  cfg <- ask
  metaDb <- R.open (metaDbPath cfg) R.defaultOptions {R.mergeOperator = R.UInt64Add}
  logId <- R.get metaDb def (serialize name)
  dataDb <- R.open (dataDbPath cfg) R.defaultOptions
  case logId of
    Nothing ->
      if createIfMissing
        then do
          newId <- generateLogId metaDb
          case newId of
            Nothing -> do
              R.close metaDb
              R.close dataDb
              return Nothing
            Just id -> do
              R.put metaDb def (serialize name) (serialize id)
              return $
                Just
                  LogHandle
                    { logName = name,
                      logID = id,
                      openOptions = op,
                      dataDb = dataDb,
                      metaDb = metaDb
                    }
        else do
          R.close metaDb
          R.close dataDb
          return Nothing
    Just id ->
      return $
        Just
          LogHandle
            { logName = name,
              logID = deserialize id,
              openOptions = op,
              dataDb = dataDb,
              metaDb = metaDb
            }

-- | append an entry to log
appendEntry :: MonadIO m => LogHandle -> Entry -> m (Maybe EntryID)
appendEntry lh@LogHandle {..} entry =
  if writeMode openOptions
    then do
      entryId <- generateEntryId metaDb logID
      case entryId of
        Nothing -> return Nothing
        Just id -> do
          R.put dataDb def (generateKey logID id) entry
          return $ Just id
    else return Nothing

-- | generate key used to append entry
generateKey :: LogID -> EntryID -> ByteString
generateKey logId entryId = append (serialize logId) (serialize entryId)

-- | generate entry Id when append success
-- |
-- | implement:
-- | like generateLogId
generateEntryId :: MonadIO m => R.DB -> LogID -> m (Maybe EntryID)
generateEntryId metaDb logId =
  do
    R.merge metaDb def maxEntryIdKey (serialize (1 :: Word64))
    newId <- R.get metaDb def maxEntryIdKey
    case newId of
      Nothing -> return Nothing
      Just id -> return $ Just $ deserialize id
  where
    maxEntryIdKey = serialize logId

-- | read entries whose entryId in [firstEntry, LastEntry]
readEntries ::
  MonadIO m =>
  LogHandle ->
  EntryID ->
  EntryID ->
  m (Maybe (ConduitT () (Maybe Entry) m ()))
readEntries LogHandle {..} firstKey lastKey = do
  source <- R.range dataDb first last
  case source of
    Nothing -> return Nothing
    Just s -> return $ Just (s .| mapC (fmap snd))
  where
    first = generateKey logID firstKey
    last = generateKey logID lastKey

-- | close log
close :: MonadIO m => LogHandle -> m ()
close LogHandle {..} = do
  R.close metaDb
  R.close dataDb
