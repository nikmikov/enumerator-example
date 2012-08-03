{-
  
  Using enumerator package to stream chunks of ByteString from file, 
  transforming them via chain of enumeratees and passing to iteratee to print result

  Known issues in this code:
  1. no QuickCheck tests
  2. doesn't handle correctly passing over midnight (will reorder packets incorrectly)
  3. doesn't handle fails in attoparsec parser (just skips the input that causes Fail )

-}

module Main(main)
where
  
import System.Environment (getArgs)
import Control.Exception (bracket)
import System.IO (hClose, openBinaryFile, IOMode(ReadMode), Handle)
import qualified Data.ByteString as B  
import qualified Data.Enumerator.Binary as EB
import Data.Enumerator as E hiding (dropWhile, head)
import qualified Data.Enumerator.List as EL
import Control.Monad.IO.Class (liftIO, MonadIO)
import Data.Attoparsec as A
import Data.Time.Clock  
import qualified Data.Set as S
import Data.List (mapAccumL)
  
import Wire



-- | default chunk size when reading data from file
--   change it to 1 to emulate slow connection
--   increasing the value will increase performance
bufferSize :: Integer
bufferSize = 4096

-- | maximum packet delay before reorder packets
maxDelay :: NominalDiffTime
maxDelay = 3 -- 3 sec


-- | enumeratee that transforms ByteString stream to QuotePacket stream
toQuotePacket :: Monad m => Enumeratee B.ByteString  QuotePacket m b
toQuotePacket =  EL.concatMapAccum (processChunk')  Nothing 
  where processChunk' Nothing     = parse' [] . A.parse parsePacket
        processChunk' (Just cont) = parse' [] . cont  
        parse' xs (Done t r)   =  if B.null t 
                                  then ( Nothing,  r:xs) 
                                  else  parse' ( r:xs )  (A.parse parsePacket t)
        parse' xs (Partial t)  =  ( Just t , xs )            
        parse' xs (Fail _ _ _) =  (Nothing, xs) 


-- | enumeratee to add packet timestamp
addPktTime ::  MonadIO m => Enumeratee QuotePacket QuotePacket m b
addPktTime  = checkDone $ continue . step 
  where step k EOF              = E.yield (Continue k) EOF
        step k (Chunks [])      = continue $ step k
        step k (Chunks chunk)   = do ct <- liftIO getCurrentTime
                                     k (Chunks $ fmap (withPktTimeSeqno ct 0) chunk)  >>== addPktTime

  

-- |  the enumeratee buffers packet within [maxDelay] sec interval and releases 
--    older packets  as chunks sorted by quoteAcceptTime
reorderPackets :: MonadIO m => Enumeratee QuotePacket QuotePacket m b
reorderPackets = checkDone $ continue . step S.empty 1
  where step mp _   k EOF         = k (Chunks (S.toList mp))  -- flush the rest of the buffer
                                    >>== checkDone (\k' -> E.yield (Continue k') EOF)
        step mp sno k (Chunks []) = continue (step mp sno k)                          
        step mp sno k (Chunks xs) = do  ct <- liftIO getCurrentTime                                     
          			        let -- add packet time and quoteTimeSeqno
                                           (newSno, xs') = mapAccumL (\acc x -> (acc + 1, withPktTimeSeqno ct acc x) ) sno xs
                                           -- add to current buffer
                                           newMp = S.union mp $ S.fromList xs' 
                                           t' = addUTCTime  ( - maxDelay) ct
                                           -- we have set ordere by quoteAcceptTime 1,2,2,3,3,4,4,4,5...
                                           -- find entry with max(quoteAcceptTime) and pktTime >= currentTime - maxDelay
                                           -- and split our ordered set by this entry
                                           resChunk = dropWhile (\x ->  t' <  pktTime x) $ S.toDescList  newMp   
                                           -- keep the rest of the buffer 
                                           leftBuf = if null resChunk then newMp else snd $ S.split (head resChunk) newMp
                                        if null resChunk 
                                          then continue (step newMp newSno k)  
                                          else k (Chunks (reverse  resChunk) )  
                                               >>== checkDone (\k' -> continue (step leftBuf newSno k' ) )



-- | iteratee to print results
printQuotes :: Iteratee QuotePacket IO ()
printQuotes = do 
  mw <- EL.head
  case mw of
    Nothing -> return()
    Just w -> do liftIO . putStrLn  . show  $ w
                 printQuotes
  
-- | enumerator that streams data  from the given handle as chunks of bytestrings
enumHandle' :: MonadIO m => Handle -> Enumerator B.ByteString m b                 
enumHandle' = EB.enumHandle bufferSize                 

                

run' :: Bool -> String -> IO()
run' r fname = bracket (openBinaryFile fname ReadMode) hClose  
               (\h -> run_ $  enumHandle' h 
                              $= toQuotePacket  
                              $= (if r then reorderPackets else addPktTime)
                              $$ printQuotes)


checkArgs :: [String] -> Maybe (Bool, String)
checkArgs ( a : [] )        = Just (False, a)
checkArgs ( "-r" : a : [] ) = Just (True, a)
checkArgs _                 = Nothing   
  
runWith :: Maybe (Bool, String) -> IO()             
runWith (Just (r,f)) = run' r f
runWith Nothing      = putStrLn "Usage: parse-quote [-r] file"   
             
main :: IO()  
main = getArgs >>= (runWith . checkArgs)

