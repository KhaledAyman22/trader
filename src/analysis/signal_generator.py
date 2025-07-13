from typing import Dict, Optional, List, Tuple
import logging
import pandas as pd

class SignalGenerator:
    def __init__(self, config: Dict):
        self.config = config
        self.strategy_config = config['strategy']
        self.tech_thresholds = self.strategy_config.get('technical_thresholds', {})
        self.flow_thresholds = self.strategy_config.get('trade_flow_thresholds', {})

    async def analyze_stock(self, stock: Dict, historical_data: List[Dict],
                          market_depth: Dict, trades_data: Dict) -> Optional[Dict]:
        try:
            from .technical import calculate_technical_indicators
            technical_indicators = calculate_technical_indicators(historical_data)

            if not technical_indicators:
                return self._create_empty_analysis(stock.get('symbol'))

            from .trade_flow import analyze_trade_flow
            trade_flow = analyze_trade_flow(trades_data, self.config)

            return self._generate_signal(stock, technical_indicators, trade_flow, market_depth, historical_data)
        except Exception as e:
            print(f"Error analyzing stock {stock.get('symbol')}: {e}")
            return None

    def _generate_signal(self, stock: Dict, technical: Dict, trade_flow: Dict, market_depth: Dict, historical_data: List[Dict]) -> Dict:
        self.stock_symbol_for_logging = stock.get('symbol', 'UNKNOWN')
        
        scores = self._calculate_component_scores(technical, trade_flow, market_depth)
        signal_type, total_score = self._determine_signal_type(scores) # Modified
        signal_strength = self._calculate_signal_strength(total_score) # Modified

        signal = {
            'symbol': stock.get('symbol'),
            'timestamp': stock.get('last_update_time'),
            'price': stock.get('last_trade_price'),
            'signal_type': signal_type,
            'signal_strength': round(signal_strength, 4), 
            'component_scores': scores,
            'technical_indicators': technical,
            'trade_flow_metrics': trade_flow,
            'risk_metrics': self._calculate_risk_metrics(stock, technical, market_depth, historical_data),
            'stock_details': stock
        }
        return signal
        
    def _calculate_component_scores(self, technical: Dict, trade_flow: Dict, market_depth: Dict) -> Dict:
        return {
            'technical': self._score_technical_indicators(technical),
            'trade_flow': self._score_trade_flow(trade_flow),
            'market_depth': self._score_market_depth(market_depth, technical.get('close', 0))
        }

    def _score_technical_indicators(self, indicators: Dict) -> int:
        def get_value(key, default=0):
            val = indicators.get(key)
            return default if val is None else val

        score = 0
        
        # --- High Importance Conditions (Score: 2) ---
        # Strong trend is active
        if get_value('adx') > self.tech_thresholds.get('adx_trend_threshold', 25):
            score += 2
        # MACD is in a bullish posture (above signal line AND histogram is positive)
        if get_value('macd') > get_value('macd_signal') and get_value('macd_hist') > self.tech_thresholds.get('macd_signal_threshold', 0):
            score += 2
        
        # --- Medium Importance Conditions (Score: 1) ---
        # RSI is not overbought
        if get_value('rsi', 50) < self.tech_thresholds.get('rsi_overbought', 70):
            score += 1
        # Stochastic is in a bullish posture
        if get_value('stoch_k') > get_value('stoch_d'):
            score += 1
        # Price is above the mid-Bollinger Band
        if get_value('close') > get_value('bb_mid'):
            score += 1
        # Price is above the 20-period SMA
        if get_value('close') > get_value('sma_20'):
            score += 1
        
        return score

    def _score_trade_flow(self, trade_flow: Dict) -> int:
        score = 0
        if trade_flow.get('buy_pressure', 0) > self.flow_thresholds.get('strong_buy_pressure', 0.65):
            score += 1
        if trade_flow.get('institutional_ratio', 0) > self.flow_thresholds.get('high_institutional_ratio', 0.60):
            score += 1
        return score

    def _score_market_depth(self, depth: Dict, current_price: float) -> int:
        score = 0
        bids_vol = depth.get('bids_vol', 0)
        asks_vol = depth.get('asks_vol', 0)
        spread = depth.get('spread', float('inf'))

        if asks_vol == 0 or current_price == 0:
            return 0

        if bids_vol > asks_vol * 1.2: # Require bids to be 20% stronger than asks
            score += 1
        if (spread / current_price) < self.strategy_config.get('max_spread_pct', 0.02):
            score += 1
        return score

    def _determine_signal_type(self, scores: Dict) -> Tuple[str, int]:
        tech_score = scores.get('technical', 0)
        flow_score = scores.get('trade_flow', 0)
        depth_score = scores.get('market_depth', 0)
        total_score = tech_score + flow_score + depth_score
        
        # Define thresholds based on the new weighted scoring
        min_tech_score = self.strategy_config.get('min_tech_conditions', 6) # Now a score, not a count
        min_flow_score = self.strategy_config.get('min_flow_conditions', 2)
        min_depth_score = self.strategy_config.get('min_depth_conditions', 1)

        is_buy = (
            tech_score >= min_tech_score and
            flow_score >= min_flow_score and
            depth_score >= min_depth_score
        )
        
        # --- DEBUG LOGGING ---
        if self.strategy_config.get('debug_mode', False):
            logging.info(
                f"[DEBUG] Symbol: {self.stock_symbol_for_logging} | "
                f"Tech: {tech_score}/{min_tech_score} | "
                f"Flow: {flow_score}/{min_flow_score} | "
                f"Depth: {depth_score}/{min_depth_score} | "
                f"Signal: {'BUY' if is_buy else 'NEUTRAL'}"
            )

        if is_buy:
            strong_buy_threshold = min_tech_score + min_flow_score + min_depth_score + 1
            if total_score >= strong_buy_threshold:
                return 'STRONG_BUY', total_score
            return 'BUY', total_score

        return 'NEUTRAL', total_score
   
    def _calculate_signal_strength(self, current_score: int) -> float:
        # Max possible score: 8 (technical) + 2 (flow) + 2 (depth) = 12
        max_possible_score = 8 + 2 + 2 
        return current_score / max_possible_score if max_possible_score > 0 else 0

    def _calculate_risk_metrics(self, stock: Dict, technical: Dict,
                              market_depth: Dict, historical_data: List[Dict]) -> Dict:
        price = stock.get('last_trade_price', 0)
        atr = technical.get('atr', 0)
        
        stop_loss_atr_multiplier = self.strategy_config.get('stop_loss_atr_multiplier', 1.5)
        take_profit_multiplier = self.strategy_config.get('take_profit_atr_multiplier', 3.0)
        
        if atr <= 0 or price <= 0:
            return {'stop_loss': 0, 'take_profit': 0, 'adjusted_buy_price': price}

        # --- Refined Stop-Loss Logic ---
        # Method 1: ATR-based stop
        atr_stop = price - (atr * stop_loss_atr_multiplier)
        
        # Method 2: Structural stop based on recent lows
        structural_stop = 0
        lookback_period = self.strategy_config.get('structural_stop_lookback', 5)
        if historical_data and len(historical_data) >= lookback_period:
            try:
                recent_lows = [p['low'] for p in historical_data[-lookback_period:]]
                structural_stop = min(recent_lows) * 0.99 # Place stop just below the lowest low
            except (KeyError, IndexError):
                structural_stop = 0
        
        # Use the more conservative (wider) stop-loss of the two methods
        stop_loss = min(atr_stop, structural_stop) if structural_stop > 0 else atr_stop

        # --- Take-Profit Calculation ---
        # Ensure a minimum 1:2 risk/reward ratio
        risk_per_share = price - stop_loss
        take_profit = price + (risk_per_share * 2)

        # --- Entry Price Adjustment ---
        # Suggest a better entry price if the current price is extended
        bb_mid = technical.get('bb_mid', 0)
        adjusted_buy_price = price
        if bb_mid > 0 and price > bb_mid:
            # Suggest entry halfway between current price and the 20-period moving average (bb_mid)
            adjusted_buy_price = (price + bb_mid) / 2

        return {
            'volatility': round(atr, 4),
            'liquidity_risk': self._calculate_liquidity_risk(market_depth),
            'position_size': self._calculate_position_size(stock),
            'stop_loss': round(stop_loss, 3),
            'take_profit': round(take_profit, 3),
            'adjusted_buy_price': round(adjusted_buy_price, 3)
        }
   
    def _calculate_liquidity_risk(self, depth: Dict) -> float:
        bids_vol = depth.get('bids_vol', 0)
        asks_vol = depth.get('asks_vol', 0)
        total_volume = bids_vol + asks_vol
        
        min_volume = self.strategy_config.get('min_daily_volume', 100000)
        return max(0, 1 - min(total_volume / min_volume, 1)) if min_volume > 0 else 1

    def _calculate_position_size(self, stock: Dict) -> float:
        price = stock.get('last_trade_price', 0)
        if not price or price <= 0:
            return 0
            
        feed_data = stock.get('feed_data', {})
        daily_volume = feed_data.get('average_daily_volume', 0)
        if daily_volume <= 0:
            return 0
        
        max_position = min(
            self.strategy_config.get('max_position_size', 100000),
            daily_volume * price * 0.1
        )
        return max(max_position, 0)

    def _calculate_stop_loss(self, technical: Dict) -> float:
        atr = technical.get('atr', 0)
        multiplier = self.strategy_config.get('stop_loss_atr_multiplier', 2)
        return atr * multiplier if atr else 0

    def _create_empty_analysis(self, symbol: Optional[str]) -> Dict:
        return {
            'symbol': symbol, 'timestamp': None, 'price': 0, 'signal_type': 'NEUTRAL',
            'signal_strength': 0, 'component_scores': {}, 'technical_indicators': {},
            'trade_flow_metrics': {}, 'risk_metrics': {},
        }