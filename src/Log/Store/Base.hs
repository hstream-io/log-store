{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE DeriveGeneric #-}
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

import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ReaderT, ask, runReaderT)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Resource (MonadUnliftIO, allocate, runResourceT)
import Data.Binary (Binary)
import qualified Data.Binary as Binary
import Data.ByteString (ByteString, append)
import qualified Data.ByteString.Lazy as BSL
import Data.Default (def)
import Data.Function ((&))
import Data.Word
import qualified Database.RocksDB as R
import GHC.Generics (Generic)
import Streamly (Serial)
import qualified Streamly.Prelude as S

-- | Log name
type LogName = String

-- | Log Id
type LogID = Word64

-- | LogHandle
data LogHandle
  = LogHandle
      { logName :: LogName,
        logID :: LogID,
        openOptions :: OpenOptions
      }

-- | entry which will be appended to a log
type Entry = ByteString

-- | entry Id
type EntryID = Word64

-- | entry content with some meta info
-- |
data InnerEntry
  = InnerEntry
      { content :: Entry,
        entryID :: EntryID
      }
  deriving (Generic)

instance Binary InnerEntry

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

dataCFName :: String
dataCFName = "data"

metaCFName :: String
metaCFName = "meta"

-- | init
--
-- | 1. get config
-- | 2. create db path if necessary
-- | 3. return db and cf handles
-- |
initialize :: MonadIO m => Env -> m Context
initialize env =
  liftIO $ do
    (db, [defaultCF, metaCF, dataCF]) <-
      R.openColumnFamilies
        R.defaultDBOptions
          { R.createIfMissing = True,
            R.createMissingColumnFamilies = True
          }
        rootPath
        [ R.ColumnFamilyDescriptor {name = "default", options = R.defaultDBOptions},
          R.ColumnFamilyDescriptor {name = metaCFName, options = R.defaultDBOptions},
          R.ColumnFamilyDescriptor {name = dataCFName, options = R.defaultDBOptions}
        ]
    return Context {dbHandle = db, defaultCFHandle = defaultCF, metaCFHandle = metaCF, dataCFHandle = dataCF}
  where
    rootPath =
      case env of
        DefaultEnv -> rootDbPath defaultConfig
        UserDefinedEnv cfg -> rootDbPath cfg

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
generateLogId :: MonadIO m => R.DB -> R.ColumnFamily -> m LogID
generateLogId db cf =
  do
    oldId <- R.getCF db def cf maxLogIdKey
    case oldId of
      Nothing -> do
        R.putCF db def cf maxLogIdKey (serialize (1 :: Word64))
        return 1
      Just oid -> updateLogId oid
  where
    maxLogIdKey = "maxLogId"
    updateLogId oldId =
      do
        R.putCF db def cf maxLogIdKey (serialize newLogId)
        return newLogId
      where
        newLogId :: Word64
        newLogId = deserialize oldId + 1

-- | serialize logId/entryId
serialize :: Binary b => b -> ByteString
serialize = BSL.toStrict . Binary.encode

-- | deserialize logId/entryId
deserialize :: Binary b => ByteString -> b
deserialize = Binary.decode . BSL.fromStrict

data Context
  = Context
      { dbHandle :: R.DB,
        defaultCFHandle :: R.ColumnFamily,
        metaCFHandle :: R.ColumnFamily,
        dataCFHandle :: R.ColumnFamily
      }

-- | open a log, will return a LogHandle for later operation
-- | (such as append and read)
open :: MonadIO m => LogName -> OpenOptions -> ReaderT Context m LogHandle
open name op@OpenOptions {..} = do
  Context {..} <- ask
  logId <- R.getCF dbHandle def metaCFHandle (serialize name)
  case logId of
    Nothing ->
      if createIfMissing
        then lift $ do
          id <- generateLogId dbHandle metaCFHandle
          R.putCF dbHandle def metaCFHandle (serialize name) (serialize id)
          -- init entryId to 0
          R.putCF dbHandle def metaCFHandle (serialize id) (serialize (0 :: Word64))
          return $
            LogHandle
              { logName = name,
                logID = id,
                openOptions = op
              }
        else liftIO $ ioError $ userError $ "no log named " ++ name ++ " found"
    Just id ->
      return $
        LogHandle
          { logName = name,
            logID = deserialize id,
            openOptions = op
          }

-- | append an entry to log
appendEntry :: MonadIO m => LogHandle -> Entry -> ReaderT Context m EntryID
appendEntry LogHandle {..} entry = do
  Context {..} <- ask
  if writeMode openOptions
    then do
      entryId <- generateEntryId dbHandle metaCFHandle logID
      saveEntry dbHandle dataCFHandle entryId
    else liftIO $ ioError $ userError $ "log named " ++ logName ++ " is not writable."
  where
    saveEntry db cf id = do
      R.putCF db def cf (generateKey logID id) (serialize InnerEntry {content = entry, entryID = id})
      return id

-- | generate key used to append entry
generateKey :: LogID -> EntryID -> ByteString
generateKey logId entryId = append (serialize logId) (serialize entryId)

-- | generate entry Id
-- |
generateEntryId :: MonadIO m => R.DB -> R.ColumnFamily -> LogID -> m EntryID
generateEntryId db cf logId =
  do
    oldId <- R.getCF db def cf maxEntryIdKey
    case oldId of
      Nothing -> liftIO $ ioError $ userError "data corrupt"
      Just oid -> updateEntryId oid
  where
    maxEntryIdKey = serialize logId
    updateEntryId oldId =
      do
        R.putCF db def cf maxEntryIdKey (serialize newEntryId)
        return newEntryId
      where
        newEntryId :: Word64
        newEntryId = deserialize oldId + 1

-- | read entries whose entryId in [firstEntry, LastEntry]
-- |
-- |
readEntries ::
  MonadIO m =>
  LogHandle ->
  Maybe EntryID ->
  Maybe EntryID ->
  ReaderT Context m (Serial (Entry, EntryID))
readEntries LogHandle {..} firstKey lastKey = do
  Context {..} <- ask
  let kvStream = R.rangeCF dbHandle def dataCFHandle first last
  return $ kvStream & S.map snd & S.map (deserialize :: ByteString -> InnerEntry) & S.map (\e -> (content e, entryID e))
  where
    first =
      case firstKey of
        Nothing -> Just $ generateKey logID 0
        Just k -> Just $ generateKey logID k
    last =
      case lastKey of
        Nothing -> Just $ generateKey logID 0xffffffffffffffff
        Just k -> Just $ generateKey logID k

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
  R.destroyColumnFamily defaultCFHandle
  R.destroyColumnFamily metaCFHandle
  R.destroyColumnFamily dataCFHandle
  R.close dbHandle

-- | function that wrap initialize and resource release.
-- |
withLogStore :: MonadUnliftIO m => Config -> ReaderT Context m a -> m a
withLogStore cfg r =
  runResourceT
    ( do
        (_, ctx) <-
          allocate
            (initialize $ UserDefinedEnv cfg)
            (runReaderT shutDown)
        lift $ runReaderT r ctx
    )
