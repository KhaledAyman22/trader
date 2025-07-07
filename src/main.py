import asyncio
import logging
from typing import Dict, List
import aiohttp
from datetime import datetime

from src.database.postgres import get_db
from src.services.market_data import MarketDataService
from src.services.telegram import TelegramService
from src.analysis.signal_generator import SignalGenerator
from src.utils.rate_limiter import RateLimiter

class TradingApp:
    def __init__(self, config: Dict):
        self.config = config
        self.rate_limiter = RateLimiter(
            max_concurrent=config.get('max_concurrent', 2),
            requests_per_minute=config['api_settings'].get('rate_limit_requests_per_minute', 60)
        )
        self.market_data = MarketDataService(config, self.rate_limiter)
        self.signal_generator = SignalGenerator(config)
        self.db = next(get_db())
        self.telegram = TelegramService(config, self.db)
        self.logger = logging.getLogger(__name__)

    async def run(self):
        self.logger.info("Starting trading application...")
        
        while True:
            try:
                await self._process_market_cycle()
                await asyncio.sleep(self.config.get('scan_interval_seconds', 300))
                logging.info("Market cycle completed successfully.")
            except Exception as e:
                self.logger.error(f"Error in market cycle: {e}")
                await self.telegram.send_alert('error', f"Market cycle failed: {str(e)}", 'high')
                await asyncio.sleep(5)

    async def _process_market_cycle(self):
        async with aiohttp.ClientSession() as session:
            # Fetch market data
            stocks = await self.market_data.fetch_market_data(
                session, 
                self.config['api_settings']['headers']
            )
            
            for stock in stocks:
                try:
                    signal = await self._analyze_stock(session, stock)
             
                    if not signal:
                        logging.info(f"No signal generated for {stock.get('symbol')}")

                    elif signal and signal['signal_type'] == 'NEUTRAL':
                        logging.info(f"Generated signal for {stock.get('symbol')}: {signal['signal_type']} last price: {stock['last_trade_price']:.2f}")
             
                    elif signal and signal['signal_type'] != 'NEUTRAL':
                        logging.info(f"Generated signal for {stock.get('symbol')}: {signal['signal_type']} last price: {stock['last_trade_price']:.2f}")
                        await self._process_signals([signal])
                    
                except Exception as e:
                    self.logger.error(f"Error analyzing {stock.get('symbol')}: {e}")
                
                finally:
                    await asyncio.sleep(0.5)


    async def _analyze_stock(self, session: aiohttp.ClientSession, stock: Dict) -> Dict:
        asset_id = stock.get('asset_id')
        if not asset_id:
            return None

        # Fetch additional data
        historical_data = await self.market_data.fetch_historical_data(
            session,
            self.config['api_settings']['headers'],
            asset_id
        )
        
        market_depth = await self.market_data.fetch_market_depth(
            session,
            self.config['api_settings']['headers'],
            asset_id
        )
        
        trades_data = await self.market_data.fetch_recent_trades(
            session,
            self.config['api_settings']['headers'],
            asset_id
        )


        return await self.signal_generator.analyze_stock(
            stock,
            historical_data,
            market_depth,
            trades_data
        )

    async def _process_signals(self, signals: List[Dict]):
        # Store signals in database
        for signal in signals:
            await self._store_signal(signal)

        # Send alerts
        await self._send_signal_alerts(signals)

    async def _store_signal(self, signal: Dict):
        try:
            # Store in PostgreSQL using SQLAlchemy
            from .database.models import SignalHistory
            
            signal_record = SignalHistory(
                symbol=signal['symbol'],
                timestamp=datetime.fromtimestamp(signal['timestamp']/1000),
                price=signal['price'],
                signal_type=signal['signal_type'],
                technical_indicators=signal['technical_indicators'],
                market_depth=signal['trade_flow_metrics'],
                trade_flow=signal['trade_flow_metrics'],
                signal_strength=signal['signal_strength']
            )
            
            self.db.add(signal_record)
            await self.db.commit()
            
        except Exception as e:
            self.logger.error(f"Failed to store signal: {e}")
            await self.db.rollback()

    async def _send_signal_alerts(self, signals: List[Dict]):
        for signal in signals:
            message = self._format_signal_message(signal)
            await self.telegram.send_alert('signal', message)

    def _format_signal_message(self, signal: Dict) -> str:
        return (
            f"*{signal['signal_type']} Signal*\n"
            f"Symbol: `{signal['symbol']}`\n"
            f"Price: `{signal['price']:.2f}`\n"
            f"Strength: `{signal['signal_strength']:.2%}`\n\n"
            f"*Technical Indicators*\n"
            f"RSI: `{signal['technical_indicators'].get('rsi', 0):.1f}`\n"
            f"MACD: `{signal['technical_indicators'].get('macd', 0):.3f}`\n"
            f"ATR: `{signal['technical_indicators'].get('atr', 0):.3f}`\n\n"
            f"*Risk Metrics*\n"
            f"Stop Loss: `{signal['risk_metrics']['stop_loss']:.2f}`\n"
            f"Position Size: `{signal['risk_metrics']['position_size']:.0f}`\n"
            f"Liquidity Risk: `{signal['risk_metrics']['liquidity_risk']:.1%}`"
        )
