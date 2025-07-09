import asyncio
import logging
from typing import Dict, List
import aiohttp
from datetime import datetime, date

from src.database.postgres import get_db
from src.services.market_data import MarketDataService
from src.services.telegram import TelegramService
from src.analysis.signal_generator import SignalGenerator
from src.utils.rate_limiter import RateLimiter


def format_market_cap(market_cap):
    """Helper function to format market cap for display."""
    if not isinstance(market_cap, (int, float)) or market_cap <= 0:
        return "N/A"
    if market_cap >= 1e9:
        return f"{market_cap / 1e9:.2f}B EGP"
    elif market_cap >= 1e6:
        return f"{market_cap / 1e6:.2f}M EGP"
    return f"{market_cap / 1e3:.2f}K EGP"
    
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
        self.sent_signals_today: Dict[str, Dict] = {}
        self.current_date: date = datetime.now().date()

    async def run(self):
        self.logger.info("Starting trading application...")
        
        while True:
            try:
                # --- CORRECT PLACEMENT ---
                # Check for new Telegram subscribers independently of the market cycle.
                await self.telegram.process_updates()
                # --- END ---

                await self._process_market_cycle()
                
                # The main sleep interval for the entire loop
                await asyncio.sleep(self.config.get('scan_interval_seconds', 10))

            except Exception as e:
                self.logger.error(f"Error in main application loop: {e}")
                # Optional: Send an alert for critical errors in the loop itself
                await self.telegram.send_alert('error', f"Critical application error: {str(e)}", 'high')
                await asyncio.sleep(60) # Wait longer after a critical failure

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
                    await asyncio.sleep(1)

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

            # --- SAFE TIMESTAMP HANDLING ---
            # If the signal timestamp is missing, use the current time as a fallback.
            signal_timestamp = signal.get('timestamp')
            if signal_timestamp is None:
                record_timestamp = datetime.now()
            else:
                record_timestamp = datetime.fromtimestamp(signal_timestamp / 1000)
            # --- END SAFE HANDLING ---
            
            risk_metrics = signal.get('risk_metrics', {})

            signal_record = SignalHistory(
                symbol=signal['symbol'],
                timestamp=record_timestamp,
                price=signal['price'],
                signal_type=signal['signal_type'],
                technical_indicators=signal['technical_indicators'],
                market_depth=signal['trade_flow_metrics'],
                trade_flow=signal['trade_flow_metrics'],
                signal_strength=signal['signal_strength'],
                target=risk_metrics.get('take_profit'),
                buy_price=risk_metrics.get('adjusted_buy_price'),
                stop_loss=risk_metrics.get('stop_loss')
            )

            self.db.add(signal_record)
            self.db.commit()

        except Exception as e:
            self.logger.error(f"Failed to store signal for {signal.get('symbol')}: {e}")
            self.db.rollback() 
    async def _send_signal_alerts(self, signals: List[Dict]):
        today = datetime.now().date()
        if today != self.current_date:
            self.sent_signals_today.clear()
            self.current_date = today

        for signal in signals:
            symbol = signal.get('symbol')
            if not symbol:
                continue

            last_signal = self.sent_signals_today.get(symbol)
            
            is_different = (
                not last_signal or
                last_signal.get('signal_type') != signal.get('signal_type') or
                abs(last_signal.get('signal_strength', 0) - signal.get('signal_strength', 0)) > 0.05
            )

            if is_different:
                is_update = last_signal is not None
                message = self._format_signal_message(signal, is_update=is_update)
                await self.telegram.send_alert('signal', message)
                self.sent_signals_today[symbol] = signal

    def _format_signal_message(self, signal: Dict, is_update: bool = False) -> str:
        # Safely extract all data from the enriched signal object
        stock_details = signal.get('stock_details', {})
        risk_metrics = signal.get('risk_metrics', {})
        tech_indicators = signal.get('technical_indicators', {})
        component_scores = signal.get('component_scores', {})

        price = signal.get('price', 0)
        signal_type = signal.get('signal_type', 'N/A').replace('_', ' ').upper()
        
        # Get the overall signal strength percentage
        overall_strength = signal.get('signal_strength', 0)

        # Get the adjusted buy price
        adjusted_buy_price = risk_metrics.get('adjusted_buy_price', 0)

        name = stock_details.get('name', 'N/A')
        symbol = stock_details.get('symbol', 'N/A')
        change_pct = stock_details.get('last_change_prc', 0)
        market_cap = format_market_cap(stock_details.get('market_cap', 0))
        sector = stock_details.get('industry', 'N/A')
        pe_ratio = stock_details.get('pe_ratio', 0)

        rsi = tech_indicators.get('rsi', 0)
        macd = tech_indicators.get('macd', 0)
        atr = tech_indicators.get('atr', 0)

        tech_score = component_scores.get('technical', 0)
        flow_score = component_scores.get('trade_flow', 0)
        depth_score = component_scores.get('market_depth', 0)

        stop_loss = risk_metrics.get('stop_loss', 0)
        take_profit = risk_metrics.get('take_profit', 0)
        
        risk_reward = 0
        if (price - stop_loss) > 0:
            risk_reward = (take_profit - price) / (price - stop_loss)

        update_note = "🔥 *UPDATE* 🔥\n" if is_update else ""

        # Build the message
        return (
            f"{update_note}🚀 *{signal_type} SIGNAL*\n"
            f"*{name} ({symbol})*\n\n"
            f"💰 *Current Price:* `{price:.2f} EGP`\n"
            f"🎯 *Entry Target:* `{adjusted_buy_price:.2f} EGP`\n"
            f"📊 *Change:* `{change_pct:.2f}%`\n"
            f"🏢 *Market Cap:* `{market_cap}`\n"
            f"🏭 *Sector:* {sector}\n"
            f"📈 *P/E:* `{pe_ratio:.2f}`\n\n"
            f"📊 *Technical Indicators:*\n"
            f"• RSI(14): `{rsi:.1f}`\n"
            f"• MACD: `{macd:.3f}`\n"
            f"• ATR: `{atr:.2f}`\n\n"
            f"🎯 *Signal Breakdown:*\n"
            f"• Technical: `{tech_score}/6`\n"
            f"• Trade Flow: `{flow_score}/2`\n"
            f"• Market Depth: `{depth_score}/2`\n\n"
            f"⭐ *Overall Strength:* `{overall_strength:.0%}`\n\n"
            f"🎯 *Exit Strategy:*\n"
            f"🔴 *Stop-Loss:* `{stop_loss:.2f} EGP`\n"
            f"🟢 *Take-Profit:* `{take_profit:.2f} EGP`\n"
            f"📏 *Risk/Reward:* `1:{risk_reward:.1f}`"
        )