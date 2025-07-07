from typing import Dict, List, Optional, Union
import numpy as np

def analyze_trade_flow(trades_data: Union[List[Dict], Dict], config: Dict) -> Dict:
    """
    Analyze trade flow patterns from trading data.
    
    Args:
        trades_data: Either a list of trades or a dict containing trades and metrics
        config: Configuration dictionary
        
    Returns:
        Dictionary containing trade flow analysis
    """
    if not trades_data:
        return create_empty_trade_flow()
    
    # Handle different input formats
    if isinstance(trades_data, list):
        trades = trades_data
        metrics = {}
    elif isinstance(trades_data, dict):
        trades = trades_data.get('trades', [])
        metrics = trades_data.get('metrics', {})
    else:
        return create_empty_trade_flow()
    
    if not trades:
        return create_empty_trade_flow()
    
    institutional_threshold = config.get('strategy', {}).get('institutional_trade_threshold')
    
    # Enhanced trade classification
    classified_trades = classify_trades(trades, institutional_threshold)
    
    # Enhanced metrics calculation
    volume_metrics = calculate_volume_metrics(classified_trades)
    price_impact = calculate_price_impact(classified_trades)
    trade_patterns = analyze_trade_patterns(classified_trades)
    
    return {
        'buy_pressure': volume_metrics['buy_pressure'],
        'sell_pressure': volume_metrics['sell_pressure'],
        'institutional_ratio': volume_metrics['institutional_ratio'],
        'price_impact': price_impact,
        'trade_patterns': trade_patterns,
        'total_trades': len(trades),
        'volume_metrics': metrics  # Include the raw metrics from fetch_recent_trades
    }

def classify_trades(trades: List[Dict], threshold: float) -> Dict:
    """
    Classify trades as institutional or retail based on value threshold.
    
    Args:
        trades: List of trade dictionaries
        threshold: Value threshold for institutional classification
        
    Returns:
        Dictionary with classified trades
    """
    institutional_trades = []
    retail_trades = []
    
    for trade in trades:
        # Calculate trade value - handle different field names
        value = get_trade_value(trade)
        
        if value >= threshold:
            institutional_trades.append(trade)
        else:
            retail_trades.append(trade)
    
    return {
        'institutional': institutional_trades,
        'retail': retail_trades
    }

def get_trade_value(trade: Dict) -> float:
    """
    Extract trade value from trade dictionary, handling different field names.
    
    Args:
        trade: Trade dictionary
        
    Returns:
        Trade value as float
    """
    # Try different common field names for trade value
    value_fields = ['value', 'amount', 'notional', 'trade_value']
    for field in value_fields:
        if field in trade and trade[field] is not None:
            try:
                return float(trade[field])
            except (ValueError, TypeError):
                continue
    
    # Calculate from price and volume if available
    price = trade.get('price', 0)
    volume = get_trade_volume(trade)
    
    if price and volume:
        return price * volume
    
    return 0

def get_trade_volume(trade: Dict) -> float:
    """
    Extract trade volume from trade dictionary, handling different field names.
    
    Args:
        trade: Trade dictionary
        
    Returns:
        Trade volume as float
    """
    # Try different common field names for volume
    volume_fields = ['volume', 'shares', 'quantity', 'size', 'qty']
    for field in volume_fields:
        if field in trade and trade[field] is not None:
            try:
                return float(trade[field])
            except (ValueError, TypeError):
                continue
    
    return 0

def get_trade_side(trade: Dict) -> str:
    """
    Extract trade side from trade dictionary, handling different field names.
    
    Args:
        trade: Trade dictionary
        
    Returns:
        Trade side as string (BUY/SELL)
    """
    # Try different common field names for trade side
    side_fields = ['side', 'type', 'direction', 'action']
    for field in side_fields:
        if field in trade and trade[field] is not None:
            side = str(trade[field]).upper()
            if side in ['BUY', 'SELL', 'B', 'S']:
                return 'BUY' if side in ['BUY', 'B'] else 'SELL'
    
    return 'UNKNOWN'

def calculate_volume_metrics(classified_trades: Dict) -> Dict:
    """
    Calculate volume-based metrics from classified trades.
    
    Args:
        classified_trades: Dictionary with institutional and retail trades
        
    Returns:
        Dictionary with volume metrics
    """
    inst_value = sum(get_trade_value(t) for t in classified_trades['institutional'])
    retail_value = sum(get_trade_value(t) for t in classified_trades['retail'])
    total_value = inst_value + retail_value
    
    return {
        'institutional_ratio': inst_value / total_value if total_value > 0 else 0,
        'buy_pressure': calculate_buy_pressure(classified_trades),
        'sell_pressure': calculate_sell_pressure(classified_trades)
    }

def calculate_buy_pressure(classified_trades: Dict) -> float:
    """
    Calculate buy pressure from classified trades.
    
    Args:
        classified_trades: Dictionary with institutional and retail trades
        
    Returns:
        Buy pressure ratio (0-1)
    """
    total_buy_volume = 0
    total_volume = 0
    
    for category in ['institutional', 'retail']:
        for trade in classified_trades[category]:
            volume = get_trade_volume(trade)
            side = get_trade_side(trade)
            
            if side == 'BUY':
                total_buy_volume += volume
            total_volume += volume
    
    return total_buy_volume / total_volume if total_volume > 0 else 0

def calculate_sell_pressure(classified_trades: Dict) -> float:
    """
    Calculate sell pressure from classified trades.
    
    Args:
        classified_trades: Dictionary with institutional and retail trades
        
    Returns:
        Sell pressure ratio (0-1)
    """
    total_sell_volume = 0
    total_volume = 0
    
    for category in ['institutional', 'retail']:
        for trade in classified_trades[category]:
            volume = get_trade_volume(trade)
            side = get_trade_side(trade)
            
            if side == 'SELL':
                total_sell_volume += volume
            total_volume += volume
    
    return total_sell_volume / total_volume if total_volume > 0 else 0

def calculate_price_impact(classified_trades: Dict) -> float:
    """
    Calculate price impact from classified trades.
    
    Args:
        classified_trades: Dictionary with institutional and retail trades
        
    Returns:
        Average price impact
    """
    price_changes = []
    
    for category in ['institutional', 'retail']:
        trades = classified_trades[category]
        # Sort trades by timestamp if available
        if trades and 'timestamp' in trades[0]:
            try:
                trades = sorted(trades, key=lambda x: x.get('timestamp', 0))
            except:
                pass
        
        for i, trade in enumerate(trades):
            if i > 0:
                prev_price = get_trade_price(trades[i-1])
                curr_price = get_trade_price(trade)
                
                if prev_price > 0 and curr_price > 0:
                    price_change = (curr_price - prev_price) / prev_price
                    price_changes.append(price_change)
    
    return float(np.mean(price_changes)) if price_changes else 0

def get_trade_price(trade: Dict) -> float:
    """
    Extract trade price from trade dictionary.
    
    Args:
        trade: Trade dictionary
        
    Returns:
        Trade price as float
    """
    price_fields = ['price', 'trade_price', 'execution_price']
    for field in price_fields:
        if field in trade and trade[field] is not None:
            try:
                return float(trade[field])
            except (ValueError, TypeError):
                continue
    
    return 0

def analyze_trade_patterns(classified_trades: Dict) -> Dict:
    """
    Analyze trading patterns from classified trades.
    
    Args:
        classified_trades: Dictionary with institutional and retail trades
        
    Returns:
        Dictionary with pattern analysis
    """
    patterns = {
        'institutional_buying': False,
        'institutional_selling': False,
        'retail_accumulation': False,
        'retail_distribution': False,
        'volume_surge': False
    }
    
    inst_trades = classified_trades['institutional']
    retail_trades = classified_trades['retail']
    
    # Check institutional patterns
    if inst_trades:
        recent_inst = inst_trades[-min(5, len(inst_trades)):]
        buy_count = sum(1 for t in recent_inst if get_trade_side(t) == 'BUY')
        sell_count = sum(1 for t in recent_inst if get_trade_side(t) == 'SELL')
        total_count = buy_count + sell_count
        
        if total_count > 0:
            buy_ratio = buy_count / total_count
            patterns['institutional_buying'] = buy_ratio > 0.7
            patterns['institutional_selling'] = buy_ratio < 0.3
    
    # Check retail patterns
    if retail_trades:
        recent_retail = retail_trades[-min(10, len(retail_trades)):]
        buy_count = sum(1 for t in recent_retail if get_trade_side(t) == 'BUY')
        sell_count = sum(1 for t in recent_retail if get_trade_side(t) == 'SELL')
        total_count = buy_count + sell_count
        
        if total_count > 0:
            buy_ratio = buy_count / total_count
            patterns['retail_accumulation'] = buy_ratio > 0.6
            patterns['retail_distribution'] = buy_ratio < 0.4
    
    # Check volume surge
    all_trades = inst_trades + retail_trades
    if len(all_trades) >= 10:
        try:
            recent_vol = sum(get_trade_volume(t) for t in all_trades[-5:])
            prev_vol = sum(get_trade_volume(t) for t in all_trades[-10:-5])
            patterns['volume_surge'] = recent_vol > prev_vol * 1.5 if prev_vol > 0 else False
        except:
            patterns['volume_surge'] = False
    
    return patterns

def create_empty_trade_flow() -> Dict:
    """
    Create an empty trade flow analysis dictionary.
    
    Returns:
        Empty trade flow dictionary
    """
    return {
        'buy_pressure': 0,
        'sell_pressure': 0,
        'institutional_ratio': 0,
        'price_impact': 0,
        'trade_patterns': {
            'institutional_buying': False,
            'institutional_selling': False,
            'retail_accumulation': False,
            'retail_distribution': False,
            'volume_surge': False
       },
        'total_trades': 0
    }