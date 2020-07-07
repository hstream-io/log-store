{-# LANGUAGE OverloadedStrings #-}

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Reader (ReaderT, runReaderT)
import Control.Monad.Trans.Resource (MonadUnliftIO, allocate, runResourceT)
import Criterion.Main
import qualified Data.ByteString as B
import Log.Store.Base
import System.IO.Temp (createTempDirectory)

main =
  defaultMain
    [ bgroup
        "append"
        [ bench "2^4 2^20" $
            nfIO
              ( withLogStoreBench $ do
                  lh <-
                    open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  append16BytesEntries lh $ 2 ^ 20
              ),
          bench "2^5 2^20" $
            nfIO
              ( withLogStoreBench $ do
                  lh <-
                    open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  append32BytesEntries lh $ 2 ^ 20
              ),
          bench "2^6 2^20" $
            nfIO
              ( withLogStoreBench $ do
                  lh <-
                    open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  append64BytesEntries lh $ 2 ^ 20
              ),
          bench "2^12 2^13" $
            nfIO
              ( withLogStoreBench $ do
                  lh <-
                    open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  append4096BytesEntries lh $ 2 ^ 13
              ),
          bench "2^12 2^14" $
            nfIO
              ( withLogStoreBench $ do
                  lh <-
                    open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  append4096BytesEntries lh $ 2 ^ 14
              ),
          bench "2^12 2^20" $
            nfIO
              ( withLogStoreBench $ do
                  lh <-
                    open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  append4096BytesEntries lh $ 2 ^ 20
              )
        ]
    ]

append16BytesEntries :: MonadIO m => LogHandle -> Int -> ReaderT Context m EntryID
append16BytesEntries lh num = append16' 1
  where
    append16' x =
      if x == num
        then appendEntry lh entry16Bytes
        else do
          appendEntry lh entry16Bytes
          append16' (x + 1)

append32BytesEntries :: MonadIO m => LogHandle -> Int -> ReaderT Context m EntryID
append32BytesEntries lh num = append32' 1
  where
    append32' x =
      if x == num
        then appendEntry lh entry32Bytes
        else do
          appendEntry lh entry32Bytes
          append32' (x + 1)

append64BytesEntries :: MonadIO m => LogHandle -> Int -> ReaderT Context m EntryID
append64BytesEntries lh num = append64' 1
  where
    append64' x =
      if x == num
        then appendEntry lh entry64Bytes
        else do
          appendEntry lh entry64Bytes
          append64' (x + 1)

append4096BytesEntries :: MonadIO m => LogHandle -> Int -> ReaderT Context m EntryID
append4096BytesEntries lh num = append4096' 1
  where
    append4096' x =
      if x == num
        then appendEntry lh entry4096Bytes
        else do
          appendEntry lh entry4096Bytes
          append4096' (x + 1)

entry16Bytes :: B.ByteString
entry16Bytes = B.replicate 2 0xff

entry32Bytes :: B.ByteString
entry32Bytes = B.replicate 4 0xff

entry64Bytes :: B.ByteString
entry64Bytes = B.replicate 8 0xff

entry4096Bytes :: B.ByteString
entry4096Bytes = B.replicate 512 0xff

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
