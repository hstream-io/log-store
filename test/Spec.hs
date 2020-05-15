{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Conduit ((.|), ConduitT, runConduit, sinkList)
import Control.Applicative (liftA3)
import Control.Monad (fmap, join)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (runReaderT)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Resource (allocate, runResourceT)
import Data.Foldable (forM_)
import Data.Word (Word64)
import Log.Store.Base
  ( Config (..),
    Entry,
    EntryID,
    Env (..),
    OpenOptions,
    appendEntry,
    close,
    createIfMissing,
    defaultOpenOptions,
    deserialize,
    generateLogId,
    initialize,
    open,
    readEntries,
    readMode,
    serialize,
    writeMode,
  )
import System.IO.Temp (createTempDirectory)
import Test.Hspec
  ( describe,
    hspec,
    it,
    shouldReturn,
  )

main :: IO ()
main = hspec $ do
  describe "Basic Functionality" $
    do
      it "should successfully put an entry" $
        runResourceT
          ( do
              (releaseKey, path) <-
                createTempDirectory
                  Nothing
                  $ "log-store-test"
              cfg <- initialize $ UserDefinedEnv Config {rootDbPath = path}
              (_, logHandle) <-
                allocate
                  ( runReaderT
                      ( open
                          "topic"
                          defaultOpenOptions {writeMode = True, createIfMissing = True}
                      )
                      cfg
                  )
                  (`forM_` close)
              liftIM1 logHandle (`appendEntry` "entry")
          )
          `shouldReturn` Just 1
      it "should successfully put some entries and read them" $
        runResourceT
          ( do
              (releaseKey, path) <-
                createTempDirectory
                  Nothing
                  $ "log-store-test"
              cfg <- initialize $ UserDefinedEnv Config {rootDbPath = path}
              (_, logHandle) <-
                allocate
                  ( runReaderT
                      ( open
                          "topic"
                          defaultOpenOptions {writeMode = True, createIfMissing = True}
                      )
                      cfg
                  )
                  (`forM_` close)
              first <- liftIM1 logHandle (`appendEntry` "entry1")
              liftIM1 logHandle (`appendEntry` "entry2")
              last <- liftIM1 logHandle (`appendEntry` "entry3")
              source <- liftIM3 logHandle first last readEntries
              liftIM1
                source
                ( \s -> do
                    r <- runConduit $ s .| sinkList
                    return $ Just r
                )
          )
          `shouldReturn` (Just ([Just "entry1", Just "entry2", Just "entry3"]))

liftIM1 :: MonadIO m => Maybe a -> (a -> m (Maybe b)) -> m (Maybe b)
liftIM1 Nothing _ = return Nothing
liftIM1 (Just a) f = f a

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
