module Log.Store.Utils where

import ByteString.StrictBuilder (builderBytes, word64BE)
import Control.Exception (throw)
import Data.Binary.Strict.Get (getWord64be, runGet)
import qualified Data.ByteString as B
import Data.Int (Int64)
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Time (nominalDiffTimeToSeconds)
import Data.Time.Clock.POSIX (POSIXTime)
import Data.Word (Word64)
import Log.Store.Exception

encodeWord64 :: Word64 -> B.ByteString
encodeWord64 = builderBytes . word64BE

decodeWord64 :: B.ByteString -> Word64
decodeWord64 bs =
  if rem /= B.empty
    then throw $ LogStoreDecodeException "input error"
    else case res of
      Left s -> throw $ LogStoreDecodeException s
      Right v -> v
  where
    (res, rem) = decode' bs
    decode' = runGet $ getWord64be

decodeText :: B.ByteString -> T.Text
decodeText = decodeUtf8

encodeText :: T.Text -> B.ByteString
encodeText = encodeUtf8

posixTimeToSeconds :: POSIXTime -> Int64
posixTimeToSeconds =
  floor . nominalDiffTimeToSeconds
