import asyncio
import itertools

from typing import List

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side

POSITION_LIMIT = 100
ORDER_LOT = 25
TICK_SIZE_IN_CENTS = 100
TAKER_FEE = 0.0002
MAKER_FEE = -0.0001
MAX_LOT = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS

class AutoTrader(BaseAutoTrader):

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bids = set()
        self.asks = set()
        self.FAK_asks = set()
        self.FAK_bids = set()
        self.FAK_ask_id = self.FAK_bid_id = 0
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0
        self.fut_bid_vol = self.fut_ask_vol = 0
        self.max_FAK_ask = self.max_PAS_bid = MINIMUM_BID
        self.min_FAK_bid = self.min_PAS_ask = MAXIMUM_ASK
        self.sell_order_size = self.buy_order_size = ORDER_LOT

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())
        if client_order_id != 0 and (client_order_id in self.bids or client_order_id in self.asks):
            self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled.

        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received hedge filled for order %d with average price %d and volume %d", client_order_id,
                         price, volume)

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """
        self.logger.info("received order book for instrument %d with sequence number %d", instrument,
                         sequence_number)
        if instrument == Instrument.FUTURE:
            
            fut_bid_price = bid_prices[0] 
            fut_ask_price = ask_prices[0]
            self.fut_bid_vol = bid_volumes[0]
            self.fut_ask_vol = ask_volumes[0]

            self.max_FAK_ask = fut_bid_price*(1+TAKER_FEE)
            self.max_PAS_bid = fut_bid_price*(1+MAKER_FEE)
            self.min_FAK_bid = fut_ask_price*(1+TAKER_FEE)
            self.min_PAS_ask = fut_ask_price*(1+MAKER_FEE)

        if instrument == Instrument.ETF:
            if ask_prices[0]<self.max_FAK_ask and ask_prices[0]>0:
                if self.ask_id != 0:
                        self.send_cancel_order(self.ask_id)
                vol = min(MAX_LOT, ask_volumes[0], POSITION_LIMIT - self.position - ORDER_LOT)
                if vol > 0:
                    self.FAK_bid_id = next(self.order_ids)
                    self.send_insert_order(self.FAK_bid_id, Side.BUY, ask_prices[0], vol, Lifespan.FILL_AND_KILL)
                    self.FAK_bids.add(self.FAK_bid_id)
            elif bid_prices[0]>self.min_FAK_bid:
                if self.bid_id != 0:
                        self.send_cancel_order(self.bid_id)
                vol = min(MAX_LOT, bid_volumes[0], POSITION_LIMIT + self.position - ORDER_LOT)
                if vol > 0:
                    self.FAK_ask_id = next(self.order_ids)
                    self.send_insert_order(self.FAK_ask_id, Side.SELL, bid_prices[0], vol, Lifespan.FILL_AND_KILL)
                    self.FAK_asks.add(self.FAK_ask_id)
            elif self.max_PAS_bid>bid_prices[0]+TICK_SIZE_IN_CENTS:
                bid_price = bid_prices[0]+TICK_SIZE_IN_CENTS
                if bid_price != self.bid_price:
                    if self.bid_id != 0:
                        self.send_cancel_order(self.bid_id)
                    self.bid_price = bid_price
                    self.bid_id = next(self.order_ids)
                    self.send_insert_order(self.bid_id, Side.BUY, self.bid_price, self.buy_order_size, Lifespan.GOOD_FOR_DAY)
                    self.bids.add(self.bid_id)
            elif self.min_PAS_ask < ask_prices[0]-TICK_SIZE_IN_CENTS:
                ask_price = ask_prices[0] - TICK_SIZE_IN_CENTS
                if ask_price != self.ask_price:
                    if self.ask_id != 0:
                        self.send_cancel_order(self.ask_id)
                    self.ask_price = ask_price
                    self.ask_id = next(self.order_ids)
                    self.send_insert_order(self.ask_id, Side.SELL, self.ask_price, self.sell_order_size, Lifespan.GOOD_FOR_DAY)
                    self.asks.add(self.ask_id)
            else:
                ask_price = int((self.min_PAS_ask + TICK_SIZE_IN_CENTS) //TICK_SIZE_IN_CENTS  * TICK_SIZE_IN_CENTS)
    
                bid_price = int(self.max_PAS_bid//TICK_SIZE_IN_CENTS  * TICK_SIZE_IN_CENTS)
                
                if ask_price != self.ask_price and self.min_PAS_ask != 0:
                    if self.ask_id != 0:
                        self.send_cancel_order(self.ask_id)
                    self.ask_price = ask_price
                    self.ask_id = next(self.order_ids)
                    self.send_insert_order(self.ask_id, Side.SELL, self.ask_price, self.sell_order_size, Lifespan.GOOD_FOR_DAY)
                   
                    self.asks.add(self.ask_id)
                if bid_price != self.bid_price and self.max_PAS_bid != 0:
                    if self.bid_id != 0:
                        self.send_cancel_order(self.bid_id)
                    self.bid_price = bid_price
                    self.bid_id = next(self.order_ids)
                    self.send_insert_order(self.bid_id, Side.BUY, self.bid_price, self.buy_order_size, Lifespan.GOOD_FOR_DAY)
                    
                    self.bids.add(self.bid_id)

                
                
                    
                

            
            



    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id,
                         price, volume)
        if client_order_id in self.bids or client_order_id in self.FAK_bids:
            self.position += volume
            self.send_hedge_order(next(self.order_ids), Side.ASK, MIN_BID_NEAREST_TICK, volume)
        elif client_order_id in self.asks or client_order_id in self.FAK_asks:
            self.position -= volume
            self.send_hedge_order(next(self.order_ids), Side.BID, MAX_ASK_NEAREST_TICK, volume)
        
        self.sell_order_size = min(ORDER_LOT, int(self.position + POSITION_LIMIT))
        self.buy_order_size = min(ORDER_LOT, int(POSITION_LIMIT - self.position))
    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d",
                         client_order_id, fill_volume, remaining_volume, fees)
        if remaining_volume == 0:
            if client_order_id == self.bid_id:
                self.bid_id = 0
            elif client_order_id == self.ask_id:
                self.ask_id = 0

            # It could be either a bid or an ask
            self.bids.discard(client_order_id)
            self.asks.discard(client_order_id)

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically when there is trading activity on the market.

        The five best ask (i.e. sell) and bid (i.e. buy) prices at which there
        has been trading activity are reported along with the aggregated volume
        traded at each of those price levels.

        If there are less than five prices on a side, then zeros will appear at
        the end of both the prices and volumes arrays.
        """
        self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
                         sequence_number)
