{-# LANGUAGE OverloadedStrings #-}
module Wire
       (
         QuotePacket (..) , 
         parsePacket,
         withPktTimeSeqno,
         withPktTime
       )
where


import qualified Data.ByteString.Char8 as B
import Data.Attoparsec as P
import qualified Data.Attoparsec.Char8 as P8
import Control.Applicative
import Data.Time.Clock
import Data.Time.Format
import Data.Time.Calendar
import System.Locale
import Text.Printf (printf)
    
data QuotePacket = QuotePacket{  
  pktTime          :: !UTCTime,
  issueCode        :: !B.ByteString, -- 12:  ISIN code 
  issueSeqno       :: !B.ByteString, --  3:  ASCII [0..9]
  marketStatusType :: !B.ByteString, --  2:  
  totalBidQuoteVol :: !B.ByteString, --  7:  
  bestBidPrice1    :: !B.ByteString, --  5:
  bestBidQty1      :: !B.ByteString, --  7:  
  bestBidPrice2    :: !B.ByteString, --  5:
  bestBidQty2      :: !B.ByteString, --  7:  
  bestBidPrice3    :: !B.ByteString, --  5:
  bestBidQty3      :: !B.ByteString, --  7:  
  bestBidPrice4    :: !B.ByteString, --  5:
  bestBidQty4      :: !B.ByteString, --  7:  
  bestBidPrice5    :: !B.ByteString, --  5:
  bestBidQty5      :: !B.ByteString, --  7:  
  totalAskQuoteVol :: !B.ByteString, --  7:  
  bestAskPrice1    :: !B.ByteString, --  5:
  bestAskQty1      :: !B.ByteString, --  7:  
  bestAskPrice2    :: !B.ByteString, --  5:
  bestAskQty2      :: !B.ByteString, --  7:  
  bestAskPrice3    :: !B.ByteString, --  5:
  bestAskQty3      :: !B.ByteString, --  7:  
  bestAskPrice4    :: !B.ByteString, --  5:
  bestAskQty4      :: !B.ByteString, --  7:  
  bestAskPrice5    :: !B.ByteString, --  5:
  bestAskQty5      :: !B.ByteString, --  7:    
  quoteAcceptTime  :: !Int,          --  8: HHMMSSuu
  quoteTimeSeqno   :: !Int           --  to handle situation when 2 packets can have same quoteAcceptTime  
  } deriving (Eq)
  


instance Show QuotePacket  where             
  show p =  B.unpack . B.concat . fmap (\f -> f p) $ 
            [
                time2BS . pktTime
              , space 
              , showQtime  
              , space  
              , issueCode
              , space  
              , bestBidQty5 , at, bestBidPrice5 
              , space                    
              , bestBidQty4 , at, bestBidPrice4                                    
              , space                                                      
              , bestBidQty3 , at, bestBidPrice3                
              , space                                                      
              , bestBidQty2 , at, bestBidPrice2
              , space                                                      
              , bestBidQty1 , at, bestBidPrice1                
              , space  
              , bestAskQty1 , at, bestAskPrice1 
              , space  
              , bestAskQty2 , at, bestAskPrice2 
              , space  
              , bestAskQty3 , at, bestAskPrice3 
              , space  
              , bestAskQty4 , at, bestAskPrice4 
              , space  
              , bestAskQty5 , at, bestAskPrice5
            ]
    where space _ = B.singleton ' '            
          at _ = B.singleton '@'            
          time2BS = B.pack . formatTime defaultTimeLocale "%H:%M:%S" 
          showQtime = B.pack . printf "%08d" . quoteAcceptTime 

instance Ord QuotePacket  where
  compare a b = compare ( (quoteAcceptTime a), (pktTime a), (quoteTimeSeqno a) )  ( (quoteAcceptTime b), (pktTime b), (quoteTimeSeqno b) )

packetHeader :: B.ByteString
packetHeader = "B6034"

findHeader :: Parser B.ByteString
findHeader = P8.skipWhile (/='B') *> string packetHeader <|> P.take 1 *> findHeader

endOfMessage :: Parser ()
endOfMessage = P.skipWhile (/=0xff)

parseBody :: Parser QuotePacket
parseBody = do issueCode'  <- P.take 12
               issueSeqno' <- P.take 3  
               marketStatusType' <- P.take 2
               totalBidQuoteVol' <- P.take 7
               bestBidPrice1'    <- P.take 5
               bestBidQty1'      <- P.take 7
               bestBidPrice2'    <- P.take 5
               bestBidQty2'      <- P.take 7
               bestBidPrice3'    <- P.take 5
               bestBidQty3'      <- P.take 7
               bestBidPrice4'    <- P.take 5
               bestBidQty4'      <- P.take 7
               bestBidPrice5'    <- P.take 5
               bestBidQty5'      <- P.take 7
               totalAskQuoteVol' <- P.take 7
               bestAskPrice1'    <- P.take 5
               bestAskQty1'      <- P.take 7
               bestAskPrice2'    <- P.take 5
               bestAskQty2'      <- P.take 7
               bestAskPrice3'    <- P.take 5
               bestAskQty3'      <- P.take 7
               bestAskPrice4'    <- P.take 5
               bestAskQty4'      <- P.take 7
               bestAskPrice5'    <- P.take 5
               bestAskQty5'      <- P.take 7
               _                 <- P.take (5 + 5*4 + 5 + 5 *4) -- skip unused fields
               quoteAcceptTime'  <- P.take 8
               let  qtime = case B.readInt quoteAcceptTime' of
                               Nothing      -> 0 
                               Just (n, bs) -> if B.null bs then n else 0

               return $! QuotePacket {
                 pktTime          = nullUtc ,        
                 quoteTimeSeqno   = 0,
                 issueCode        = issueCode',
                 issueSeqno       = issueSeqno',
                 marketStatusType = marketStatusType',
                 totalBidQuoteVol = totalBidQuoteVol',
                 bestBidPrice1    = bestBidPrice1',
                 bestBidQty1      = bestBidQty1',
                 bestBidPrice2    = bestBidPrice2',
                 bestBidQty2      = bestBidQty2',
                 bestBidPrice3    = bestBidPrice3',
                 bestBidQty3      = bestBidQty3',
                 bestBidPrice4    = bestBidPrice4',
                 bestBidQty4      = bestBidQty4',
                 bestBidPrice5    = bestBidPrice5',
                 bestBidQty5      = bestBidQty5',            
                 totalAskQuoteVol = totalAskQuoteVol',                 
                 bestAskPrice1    = bestAskPrice1',
                 bestAskQty1      = bestAskQty1',
                 bestAskPrice2    = bestAskPrice2',
                 bestAskQty2      = bestAskQty2',
                 bestAskPrice3    = bestAskPrice3',
                 bestAskQty3      = bestAskQty3',
                 bestAskPrice4    = bestAskPrice4',
                 bestAskQty4      = bestAskQty4',
                 bestAskPrice5    = bestAskPrice5',
                 bestAskQty5      = bestAskQty5',
                 quoteAcceptTime  = qtime
                 }
                 
withPktTime :: UTCTime -> QuotePacket -> QuotePacket                 
withPktTime t qp = qp {pktTime = t}

withPktTimeSeqno :: UTCTime -> Int -> QuotePacket -> QuotePacket                 
withPktTimeSeqno t seqno qp = qp {pktTime = t, quoteTimeSeqno = seqno}


parsePacket :: P.Parser QuotePacket
parsePacket  = findHeader *> parseBody <* endOfMessage

nullUtc :: UTCTime
nullUtc = UTCTime {utctDay = fromGregorian 0 0 0, utctDayTime = secondsToDiffTime 0}

