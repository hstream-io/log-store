module Log.Store.Utils where

import Control.Exception (throw)
import qualified Data.ByteString as B
import Data.Store (Store, decode, encode)

deserialize :: Store a => B.ByteString -> a
deserialize bytes =
  case decode bytes of
    Left e -> throw e
    Right v -> v

serialize :: Store a => a -> B.ByteString
serialize = encode
