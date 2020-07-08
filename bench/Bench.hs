{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Reader (ReaderT, runReaderT)
import Control.Monad.Trans.Resource (MonadUnliftIO, allocate, runResourceT)
import Criterion.Main
import qualified Data.ByteString as B
import qualified Data.Vector as V
import Log.Store.Base
import System.IO.Temp (createTempDirectory)

main =
  defaultMain
    [ bgroup
        "append-single"
        [ bench "2^4 2^20" $
            nfIO $
              writeNBytesEntries (2 ^ 4) (2 ^ 20),
          bench "2^5 2^20" $
            nfIO $
              writeNBytesEntries (2 ^ 5) (2 ^ 20),
          bench "2^6 2^20" $
            nfIO $
              writeNBytesEntries (2 ^ 6) (2 ^ 20),
          bench "2^7 2^20" $
            nfIO $
              writeNBytesEntries (2 ^ 7) (2 ^ 20),
          bench "2^12 2^13" $
            nfIO $
              writeNBytesEntries (2 ^ 12) (2 ^ 13),
          bench "2^12 2^14" $
            nfIO $
              writeNBytesEntries (2 ^ 12) (2 ^ 14),
          bench "2^12 2^20" $
            nfIO $
              writeNBytesEntries (2 ^ 12) (2 ^ 20)
        ],
      bgroup
        "append-batch"
        [ bench "2^5 2^7 2^13" $
            nfIO $
              writeNBytesEntriesBatch (2 ^ 4) (2 ^ 7) (2 ^ 13),
          bench "2^6 2^7 2^13" $
            nfIO $
              writeNBytesEntriesBatch (2 ^ 6) (2 ^ 7) (2 ^ 13),
          bench "2^7 2^7 2^13" $
            nfIO $
              writeNBytesEntriesBatch (2 ^ 7) (2 ^ 7) (2 ^ 13)
        ]
    ]

nBytesEntry :: Int -> B.ByteString
nBytesEntry n = B.replicate n 0xff

writeNBytesEntries :: MonadUnliftIO m => Int -> Int -> m EntryID
writeNBytesEntries entrySize entryNum =
  withLogStoreBench $ do
    lh <-
      open
        "log"
        defaultOpenOptions {writeMode = True, createIfMissing = True}
    write' lh 1
  where
    write' lh x =
      if x == entryNum
        then appendEntry lh entry
        else do
          appendEntry lh entry
          write' lh (x + 1)
    entry = nBytesEntry entrySize

writeNBytesEntriesBatch :: MonadUnliftIO m => Int -> Int -> Int -> m (V.Vector EntryID)
writeNBytesEntriesBatch entrySize batchSize batchNum =
  withLogStoreBench $ do
    lh <-
      open
        "log"
        defaultOpenOptions {writeMode = True, createIfMissing = True}
    write' lh 1
  where
    write' lh x =
      if x == batchNum
        then appendEntries lh $ V.replicate batchSize entry
        else do
          appendEntries lh $ V.replicate batchSize entry
          write' lh (x + 1)
    entry = nBytesEntry entrySize

withLogStoreBench :: MonadUnliftIO m => ReaderT Context m a -> m a
withLogStoreBench r =
  runResourceT
    ( do
        (_, path) <-
          createTempDirectory Nothing "log-store-bench"
        (_, ctx) <-
          allocate
            (initialize $ Config {rootDbPath = path})
            (runReaderT shutDown)
        lift $ runReaderT r ctx
    )
