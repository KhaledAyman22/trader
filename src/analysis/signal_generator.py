from typing import Dict, Optional, List, Tuple
from .technical import calculate_technical_indicators
from .trade_flow import analyze_trade_flow

class SignalGenerator:
    def __init__(self, config: Dict):
        self.config = config
        self.strategy_config = config['strategy']

    async def analyze_stock(self, stock: Dict, historical_data: List[Dict], 
                          market_depth: Dict, trades_data: List[Dict]) -> Optional[Dict]:
        try:
            # Technical Analysis
            technical_indicators = calculate_technical_indicators(historical_data)
            if not technical_indicators:
                # Log this event, as it might indicate data issues
                return self._create_empty_analysis(stock.get('symbol'))

            # Trade Flow Analysis
            trade_flow = analyze_trade_flow(trades_data.get('trades', []), self.config)

            # Combine all analyses
            return self._generate_signal(stock, technical_indicators, trade_flow, market_depth)
        except Exception as e:
            # Log the exception for debugging
            print(f"Error analyzing stock {stock.get('symbol')}: {e}")
            return None

    def _generate_signal(self, stock: Dict, technical: Dict, trade_flow: Dict, market_depth: Dict) -> Dict:
        scores = self._calculate_component_scores(technical, trade_flow, market_depth)
        signal_strength = self._calculate_signal_strength(scores)
        
        return {
            'symbol': stock.get('symbol'),
            'timestamp': stock.get('last_update_time'),
            'price': stock.get('last_trade_price'),
            'signal_type': self._determine_signal_type(scores, signal_strength),
            'signal_strength': signal_strength,
            'component_scores': scores,
            'technical_indicators': technical,
            'trade_flow_metrics': trade_flow,
            'risk_metrics': self._calculate_risk_metrics(stock, technical, market_depth),
        }

    def _calculate_component_scores(self, technical: Dict, trade_flow: Dict, market_depth: Dict) -> Dict:
        return {
            'technical': self._score_technical_indicators(technical),
            'trade_flow': self._score_trade_flow(trade_flow),
            'market_depth': self._score_market_depth(market_depth)
        }

    def _score_technical_indicators(self, indicators: Dict) -> float:
        weights = self.strategy_config.get('technical_weights', {
            'trend': 0.4,
            'momentum': 0.3,
            'volatility': 0.3
        })

        trend_score = self._calculate_trend_score(indicators)
        momentum_score = self._calculate_momentum_score(indicators)
        volatility_score = self._calculate_volatility_score(indicators)

        return (trend_score * weights['trend'] +
                momentum_score * weights['momentum'] +
                volatility_score * weights['volatility'])

    def _calculate_trend_score(self, indicators: Dict) -> float:
        """Calculate trend score based on technical indicators"""
        score = 0
        count = 0
        
        # MACD analysis
        macd = indicators.get('macd')
        macd_signal = indicators.get('macd_signal')
        if macd is not None and macd_signal is not None:
            score += 1 if macd > macd_signal else -1
            count += 1
        
        # ADX for trend strength
        adx = indicators.get('adx')
        if adx is not None and adx > self.strategy_config.get('adx_trend_threshold', 25):
            score += 0.5
            count += 1
        
        # Moving averages (using close vs bollinger middle as proxy)
        close = indicators.get('close')
        bb_mid = indicators.get('bb_mid')
        if close is not None and bb_mid is not None:
            score += 1 if close > bb_mid else -1
            count += 1
        
        return (score / count + 1) / 2 if count > 0 else 0.5  # Normalize to 0-1

    def _calculate_momentum_score(self, indicators: Dict) -> float:
        """Calculate momentum score based on technical indicators"""
        score = 0
        count = 0
        
        # RSI analysis
        rsi = indicators.get('rsi')
        if rsi is not None:
            overbought = self.strategy_config.get('rsi_overbought_threshold', 70)
            oversold = self.strategy_config.get('rsi_oversold_threshold', 30)
            if rsi > overbought:
                score -= 1  # Overbought is a bearish signal
            elif rsi < oversold:
                score += 1  # Oversold is a bullish signal
            else:
                score += (rsi - 50) / 20  # Normalize around 50
            count += 1
        
        # Stochastic analysis
        stoch_k = indicators.get('stoch_k')
        stoch_d = indicators.get('stoch_d')
        if stoch_k is not None and stoch_d is not None:
            score += 1 if stoch_k > stoch_d and stoch_k < 80 else -1
            count += 1
        
        # MACD histogram
        macd_hist = indicators.get('macd_hist')
        if macd_hist is not None:
            score += 1 if macd_hist > 0 else -1
            count += 1
        
        return (score / count + 1) / 2 if count > 0 else 0.5  # Normalize to 0-1

    def _calculate_volatility_score(self, indicators: Dict) -> float:
        """Calculate volatility score based on technical indicators"""
        score = 0
        count = 0
        
        # Bollinger Bands position
        close = indicators.get('close', 0)
        bb_upper = indicators.get('bb_upper', 0)
        bb_lower = indicators.get('bb_lower', 0)
        bb_mid = indicators.get('bb_mid', 0)
        
        if close and bb_upper and bb_lower and bb_mid:
            bb_position = (close - bb_lower) / (bb_upper - bb_lower)
            score += bb_position
            count += 1
        
        # ATR relative to price (lower ATR relative to price is better)
        atr = indicators.get('atr', 0)
        if close and atr and close > 0:
            atr_ratio = atr / close
            score += max(0, 1 - atr_ratio * 10)  # Prefer lower volatility
            count += 1
        
        return score / count if count > 0 else 0.5

    def _score_trade_flow(self, trade_flow: Dict) -> float:
        weights = self.strategy_config.get('trade_flow_weights', {
            'buy_pressure': 0.4,
            'institutional': 0.4,
            'price_impact': 0.2
        })

        buy_pressure = trade_flow.get('buy_pressure', 0)
        institutional_ratio = trade_flow.get('institutional_ratio', 0)
        price_impact = max(0, trade_flow.get('price_impact', 0))  # Ensure non-negative

        return (buy_pressure * weights['buy_pressure'] +
                institutional_ratio * weights['institutional'] +
                price_impact * weights['price_impact'])

    def _score_market_depth(self, depth: Dict) -> float:
        bids_vol = depth.get('bids_vol', 0)
        asks_vol = depth.get('asks_vol', 0)
        spread = depth.get('spread', float('inf'))
        
        if asks_vol == 0 or spread == float('inf'):
            return 0

        bid_ask_ratio = bids_vol / asks_vol
        spread_factor = 1 / (1 + spread) if spread > 0 else 0

        return min(bid_ask_ratio * spread_factor, 1.0)

    def _calculate_signal_strength(self, scores: Dict) -> float:
        weights = self.strategy_config.get('signal_weights', {
            'technical': 0.4,
            'trade_flow': 0.2,
            'market_depth': 0.1
        })

        total_score = sum(score * weights.get(component, 0) 
                         for component, score in scores.items()
                         if component in weights)
        
        return min(max(total_score, 0), 1)

    def _determine_signal_type(self, scores: Dict, strength: float) -> str:
        min_strength = self.strategy_config.get('min_signal_strength', 0.7)
        
        if strength < min_strength:
            return 'NEUTRAL'
        
        technical_score = scores.get('technical', 0)
        flow_score = scores.get('trade_flow', 0)
        
        technical_bullish = technical_score > 0.6
        flow_positive = flow_score > 0.6
        
        if technical_bullish and flow_positive:
            return 'STRONG_BUY'
        elif technical_bullish or flow_positive:
            return 'BUY'
        elif technical_score < 0.4 and flow_score < 0.4:
            return 'SELL'
        
        return 'NEUTRAL'

    def _calculate_risk_metrics(self, stock: Dict, technical: Dict, 
                              market_depth: Dict) -> Dict:
        return {
            'volatility': technical.get('atr', 0),
            'liquidity_risk': self._calculate_liquidity_risk(market_depth),
            'position_size': self._calculate_position_size(stock),
            'stop_loss': self._calculate_stop_loss(technical)
        }

    def _calculate_liquidity_risk(self, depth: Dict) -> float:
        bids_vol = depth.get('bids_vol', 0)
        asks_vol = depth.get('asks_vol', 0)
        total_volume = bids_vol + asks_vol
        
        min_volume = self.strategy_config.get('min_daily_volume', 100000)
        return max(0, 1 - min(total_volume / min_volume, 1)) if min_volume > 0 else 1

    def _calculate_position_size(self, stock: Dict) -> float:
        price = stock.get('last_trade_price', 0)
        market_cap = stock.get('market_cap', 0)
        
        if not price or price <= 0:
            return 0
            
        feed_data = stock.get('feed_data', {})
        daily_volume = feed_data.get('average_daily_volume', 0)
        
        if daily_volume <= 0:
            return 0
        
        max_position = min(
            self.strategy_config.get('max_position_size', 100000),
            daily_volume * price * 0.1  # Max 10% of daily volume
        )
        
        return max(max_position, 0)

    def _calculate_stop_loss(self, technical: Dict) -> float:
        atr = technical.get('atr', 0)
        multiplier = self.strategy_config.get('stop_loss_atr_multiplier', 2)
        return atr * multiplier if atr else 0

    def _create_empty_analysis(self, symbol: Optional[str]) -> Dict:
        return {
            'symbol': symbol,
            'timestamp': None,
            'price': 0,
            'signal_type': 'NEUTRAL',
            'signal_strength': 0,
            'component_scores': {
                'technical': 0,
                'trade_flow': 0,
                'market_depth': 0
            },
            'technical_indicators': {},
            'trade_flow_metrics': {},
            'risk_metrics': {
                'volatility': 0,
                'liquidity_risk': 1,
                'position_size': 0,
                'stop_loss': 0
            },
        }
