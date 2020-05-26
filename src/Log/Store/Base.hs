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
    liftIM1,
    liftIM2,
    liftIM3
  )
where

import Conduit ((.|), Conduit, ConduitT, mapC)
import Control.Exception (bracket)
import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ReaderT, ask, runReaderT)
import Control.Monad.Trans.Resource (MonadResource, allocate, runResourceT, ResourceT, MonadUnliftIO)
import Data.Binary (Binary)
import qualified Data.Binary as Binary
import Data.ByteString (ByteString, append)
import qualified Data.ByteString.Lazy as BSL
import Data.Default (def)
import Data.Word
import qualified Database.RocksDB as R
import System.FilePath.Posix ((</>))
import Control.Monad.Trans (lift)

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

data Context
  = Context
      { metaDbHandle :: R.DB,
        dataDbHandle :: R.DB
      }

-- | open a log, will return a LogHandle for later operation
-- | (such as append and read)
open :: MonadIO m => LogName -> OpenOptions -> ReaderT Context m (Maybe LogHandle)
open name op@OpenOptions {..} = do
  Context {metaDbHandle = mh, dataDbHandle = dh} <- ask
  logId <- R.get mh def (serialize name)
  case logId of
    Nothing ->
      if createIfMissing
        then do
          newId <- generateLogId mh
          liftIM1
            newId
            ( \id -> do
                R.put mh def (serialize name) (serialize id)
                return $
                  Just
                    LogHandle
                      { logName = name,
                        logID = id,
                        openOptions = op,
                        dataDb = dh,
                        metaDb = mh
                      }
            )
        else return Nothing
    Just id ->
      return $
        Just
          LogHandle
            { logName = name,
              logID = deserialize id,
              openOptions = op,
              dataDb = dh,
              metaDb = mh
            }

-- | append an entry to log
appendEntry :: MonadIO m => LogHandle -> Entry -> ReaderT Context m (Maybe EntryID)
appendEntry lh@LogHandle {..} entry =
  liftIO $
    if writeMode openOptions
      then do entryId <- generateEntryId metaDb logID
              liftIM1 entryId saveEntry
      else return Nothing
  where
    saveEntry :: EntryID -> IO (Maybe EntryID)
    saveEntry id = do
      R.put dataDb def (generateKey logID id) entry
      return $ Just id

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
  ReaderT Context m (Maybe (ConduitT () (Maybe Entry) IO ()))
readEntries LogHandle {..} firstKey lastKey = liftIO $ do
  source <- R.range dataDb first last
  liftIM1 source (\s -> return $ Just (s .| mapC (fmap snd)))
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
  Context{..} <- ask
  R.close metaDbHandle
  R.close dataDbHandle

-- | autoRelease
withLogStore :: MonadUnliftIO m => Config -> ReaderT Context (ResourceT m) a -> m a
withLogStore cfg r =  
  runResourceT
    ( do
        (_, ctx) <-
          allocate
            (initialize $ UserDefinedEnv cfg)
            (runReaderT shutDown)
        runReaderT r ctx
    )

liftIM1 :: MonadIO m => Maybe a -> (a -> m (Maybe b)) -> m (Maybe b)
liftIM1 Nothing _ = return Nothing
liftIM1 (Just a) f = f a

liftIM2 ::
  MonadIO m =>
  Maybe a ->
  Maybe b ->
  (a -> b -> m (Maybe c)) ->
  m (Maybe c)
liftIM2 Nothing _ _ = return Nothing
liftIM2 _ Nothing _ = return Nothing
liftIM2 (Just a) (Just b) f = f a b

liftIM3 ::
  MonadIO m =>
  Maybe a ->
  Maybe b ->
  Maybe c ->
  (a -> b -> c -> m (Maybe d)) ->
  m (Maybe d)
liftIM3 Nothing _ _ _ = return Nothing
liftIM3 _ Nothing _ _ = return Nothing
liftIM3 _ _ Nothing _ = return Nothing
liftIM3 (Just a) (Just b) (Just c) f = f a b c
