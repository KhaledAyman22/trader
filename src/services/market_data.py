from datetime import datetime, timedelta
import logging
import aiohttp
import asyncio
from typing import List, Dict, Optional
from ..utils.rate_limiter import RateLimiter

class MarketDataService:
    def __init__(self, config: Dict, rate_limiter: RateLimiter):
        self.config = config
        self.rate_limiter = rate_limiter
        self.base_url = "https://prod.thndr.app/assets-service"
        self.logger = logging.getLogger(__name__)
        
    async def fetch_json(self, session: aiohttp.ClientSession, url: str, headers: Dict) -> Optional[Dict]:
        try:
            async with self.rate_limiter:
                timeout = aiohttp.ClientTimeout(total=self.config['api_settings'].get('request_timeout_seconds', 30))
                async with session.get(url, headers=headers, timeout=timeout) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        except aiohttp.ClientError as e:
            self.logger.error(f"API request failed for {url}: {e}")
            return None
        except asyncio.TimeoutError:
            self.logger.error(f"API request timed out for {url}")
            return None
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred during API request for {url}: {e}")
            return None

    async def fetch_market_data(self, session: aiohttp.ClientSession, headers: Dict) -> List[Dict]:
        url = f"{self.base_url}/assets/marketwatch?market=egypt"
        data = await self.fetch_json(session, url, headers)
        
        if not data:
            return []
        
        stocks = [x for x in data.get('assets', []) if x.get("market_id") == "NOPL"]
        return await self._enhance_stocks_data(session, headers, stocks)

    async def _enhance_stocks_data(self, session: aiohttp.ClientSession, headers: Dict, stocks: List[Dict]) -> List[Dict]:
        batch_size = self.config.get('max_concurrent', 10)
        enhanced_stocks = []
        
        for i in range(0, len(stocks), batch_size):
            self.logger.info(f"📦 Processing batch {i//batch_size + 1}/{(len(stocks) + batch_size - 1)//batch_size}")
            batch = stocks[i:i + batch_size]
            tasks = [self._fetch_stock_details(session, headers, stock) for stock in batch]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, dict):
                    enhanced_stocks.append(result)
                elif isinstance(result, Exception):
                    self.logger.error(f"Error fetching stock details: {result}")

        return self._apply_filters(enhanced_stocks)

    async def _fetch_stock_details(self, session: aiohttp.ClientSession, headers: Dict, stock: Dict) -> Optional[Dict]:
        asset_id = stock.get('asset_id')
        if not asset_id:
            return None
            
        url = f"{self.base_url}/assets/{asset_id}?include_feed=true&feed_detail=true"
        data = await self.fetch_json(session, url, headers)
        
        if not data:
            return None
            
        return self._merge_stock_data(stock, data)

    async def fetch_historical_data(self, session: aiohttp.ClientSession, headers: Dict, asset_id: str) -> List[Dict]:
        # --- Market-Aware Timestamp Calculation ---
        now_dt = datetime.now()
        market_close_dt = now_dt.replace(hour=14, minute=30, second=0, microsecond=0)

        # If the script is run after market close, set the end time to the closing bell.
        # Otherwise, use the current time.
        if now_dt > market_close_dt:
            end_dt = market_close_dt
        else:
            end_dt = now_dt
            
        to_timestamp = int(end_dt.timestamp() * 1000)

        # Get lookback period from config
        lookback_minutes = self.config.get('historical_lookback_minutes', 150)
        start_dt = end_dt - timedelta(minutes=lookback_minutes)
        from_timestamp = int(start_dt.timestamp() * 1000)
        
        resolution = self.config.get('chart_resolution', 'five_minutes')
        # --- End Market-Aware Logic ---

        url = f"{self.base_url}/charts/advanced?asset_id={asset_id}&resolution={resolution}&from_timestamp={from_timestamp}&to_timestamp={to_timestamp}"
        data = await self.fetch_json(session, url, headers)
        
        # Log the request for debugging if no points are returned
        if not data or not data.get("points"):
            self.logger.warning(f"No historical points returned for {asset_id} with URL: {url}")
            return []
            
        return data.get("points", [])

    async def fetch_market_depth(self, session: aiohttp.ClientSession, headers: Dict, asset_id: str) -> Dict:
        url = f"{self.base_url}/market-depth/{asset_id}"
        data = await self.fetch_json(session, url, headers)
        
        if not data:
            return {'bids_vol': 0, 'asks_vol': 0, 'spread': 0}
            
        return self._calculate_depth_metrics(data)

    async def fetch_recent_trades(self, session: aiohttp.ClientSession, headers: Dict, asset_id: str) -> Dict:
        """Fetch recent trades for a given asset and return trades and metrics."""
        url = f"{self.base_url}/market-depth/v2/trades-book/{asset_id}?page_size=50"
        data = await self.fetch_json(session, url, headers)
        
        if not data or 'trades' not in data:
            return {'trades': [], 'metrics': {}}
            
        trades = data.get('trades', [])
        
        if not trades:
            return {'trades': [], 'metrics': {}}

        total_volume = sum(trade.get('volume', 0) for trade in trades)
        buy_volume = sum(trade.get('volume', 0) for trade in trades if trade.get('side') == 'BUY')
        sell_volume = total_volume - buy_volume
        
        return {
            'trades': trades,
            'metrics': {
                'total_volume': total_volume,
                'buy_volume': buy_volume,
                'sell_volume': sell_volume,
                'buy_ratio': buy_volume / total_volume if total_volume > 0 else 0,
                'average_price': sum(t.get('price', 0) * t.get('volume', 0) for t in trades) / total_volume if total_volume > 0 else 0,
                'price_range': {
                    'high': max(t.get('price', 0) for t in trades),
                    'low': min(t.get('price', 0) for t in trades)
                }
            }
        }

    def _merge_stock_data(self, base_stock: Dict, detailed_data: Dict) -> Dict:
        enhanced_stock = base_stock.copy()
        enhanced_stock.update({
            'market_cap': detailed_data.get('feed', {}).get('market_cap'),
            'symbol': detailed_data.get('symbol'),
            'name': detailed_data.get('name'),
            'industry': detailed_data.get('industry'),
            'feed_data': detailed_data.get('feed', {})
        })
        return enhanced_stock

    def _calculate_depth_metrics(self, depth_data: Dict) -> Dict:
        bids = depth_data.get('bids_per_price', [])
        asks = depth_data.get('asks_per_price', [])
        
        bids_vol = sum(b.get('volume_traded', 0) for b in bids)
        asks_vol = sum(a.get('volume_traded', 0) for a in asks)
        
        spread = 0
        if bids and asks:
            best_bid = max(bids, key=lambda x: x.get('order_price', 0))
            best_ask = min(asks, key=lambda x: x.get('order_price', float('inf')))
            spread = best_ask.get('order_price', 0) - best_bid.get('order_price', 0)
            
        return {
            'bids_vol': bids_vol,
            'asks_vol': asks_vol,
            'spread': spread
        }

    def _apply_filters(self, stocks: List[Dict]) -> List[Dict]:
        filtered_stocks = []
        for stock in stocks:
            if self._meets_criteria(stock):
                filtered_stocks.append(stock)
        return filtered_stocks

    def _meets_criteria(self, stock: Dict) -> bool:
        strategy_config = self.config['strategy']
        price = stock.get('last_trade_price', 0)
        market_cap = stock.get('market_cap', 0)
        symbol = stock.get('symbol', '')
        
        return (
            strategy_config.get('min_price', 0) <= price <= strategy_config.get('max_price', float('inf')) and
            market_cap >= strategy_config.get('min_market_cap', 0) and
            symbol not in strategy_config.get('blacklist_symbols', [])
        )
