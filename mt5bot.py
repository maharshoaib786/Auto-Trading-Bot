# mt5_grid_martingale.py  (v103.4-USER-BASE-SKIP-INVALID-PRICE-LEVEL)
"""
Grid-Martingale bot for MetaTrader 5 with phased Fibonacci and dynamic multipliers.
Uses LIMIT ORDERS for grid entries, with a configurable max number of pending grid orders per side.
Resets a side (cancels pending, resets Fib sequence) when any position on that side closes.
Console logging set to INFO. MT5 comments simplified.
If a limit order placement fails due to "Invalid Price" (10015), the bot advances its target
to the next grid level for that side instead of getting stuck.

• Phased lot sizing.
• Grid steps in pips.
• Grid entries via Limit Orders.
• Max Pending Grid Orders.
• Reset on Closure.
• Dynamic profit pips.
• Delay between entries.
• Auto-hedge (market orders).
• Profit Target (market orders).
• Proactive TP Sync.
• Configurable via .env.
"""
from __future__ import annotations
import os, sys, time, math, logging, traceback
import MetaTrader5 as mt5
from dotenv import load_dotenv

# Ensure UTF-8 encoding on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()


# --- Configuration ---
SYMBOL                    = os.getenv("SYMBOL", "XAUUSDm")
LOT_SMALL                 = float(os.getenv("LOT_SMALL", 0.01))
LOT_MAX                   = float(os.getenv("LOT_MAX", 20))
GRID_STEP_PIPS            = float(os.getenv("GRID_STEP_PIPS", 50))
DYNAMIC_STEP_PIPS         = float(os.getenv("DYNAMIC_STEP_PIPS", 50))
PROFIT_PIPS               = float(os.getenv("PROFIT_PIPS", 110))
DYNAMIC_PROFIT_PIPS       = float(os.getenv("DYNAMIC_PROFIT_PIPS", 160))
DYNAMIC_POSITIONS_TRIGGER = int(os.getenv("DYNAMIC_POSITIONS_TRIGGER", 13))
MAX_POSITIONS             = int(os.getenv("MAX_POS", 60)) # Max total open positions
PROFIT_TARGET_AMT         = float(os.getenv("PROFIT_TARGET_AMT", 300)) 
OPEN_DELAY                = float(os.getenv("OPEN_DELAY", 5)) 
LOOP_MS                   = int(os.getenv("LOOP_MS", 100)) 
FIB_LIMIT                 = int(os.getenv("FIB_LIMIT", 13)) 
DYNAMIC_MULTIPLIER        = float(os.getenv("DYNAMIC_MULTIPLIER", 1.25)) 
LOG_FILE                  = os.getenv("LOG_FILE", "grid_bot.log")
LOGIN                     = int(os.getenv("MT5_LOGIN", 0)) 
PASSWORD                  = os.getenv("MT5_PASSWORD", "") 
SERVER                    = os.getenv("MT5_SERVER", "") 
BOT_NAME                  = os.getenv("BOT_NAME", "MAHARSHOAIBBOT") 
PENDING_ORDER_EXPIRATION_MIN = int(os.getenv("PENDING_ORDER_EXPIRATION_MIN", 0))
MAX_PENDING_GRID_ORDERS_PER_SIDE = int(os.getenv("MAX_PENDING_GRID_ORDERS_PER_SIDE", 1))
LOG_BALANCE_INTERVAL      = int(os.getenv("LOG_BALANCE_INTERVAL", 100)) 


# --- Logging setup ---
log = logging.getLogger("grid_bot")
log.setLevel(logging.DEBUG) 

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO) 
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_handler.setLevel(logging.INFO) 
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

log.propagate = False 


# --- State variables ---
initial_equity: float      = 0.0       
last_buy_lot: float        = LOT_SMALL 
last_sell_lot: float       = LOT_SMALL 
next_buy_px: float | None  = None      
next_sell_px: float | None = None      
just_hedged: bool          = False     
prev_buy_count: int        = 0       
prev_sell_count: int       = 0       
buy_fib_index: int         = 1       
sell_fib_index: int        = 1       
loop_counter: int          = 0 

# --- Utilities ---
def fib(n: int) -> int:
    if n <= 0: return 0 
    if n == 1: return 1
    a, b = 1, 1 
    for _ in range(2, n): 
        a, b = b, a + b
    return b 

def mt5_login():
    global initial_equity 
    log.info("Attempting to initialize MetaTrader 5...")
    if not mt5.initialize(login=LOGIN, password=PASSWORD, server=SERVER, timeout=10000): 
        log.error(f"MT5 initialize() failed, error code = {mt5.last_error()}") 
        mt5.shutdown() 
        sys.exit(1) 

    log.info(f"MetaTrader 5 initialized successfully. Terminal version: {mt5.version()}")
    account_info = mt5.account_info()
    if account_info is None:
        log.error(f"Failed to get account info, error code = {mt5.last_error()}") 
        mt5.shutdown()
        sys.exit(1)
    log.info(f"Logged in to account: {account_info.login}, Server: {account_info.server}, Balance: {account_info.balance:.2f} {account_info.currency}")


    log.info(f"Selecting symbol: {SYMBOL}")
    if not mt5.symbol_select(SYMBOL, True):
        log.error(f"Symbol {SYMBOL} not found or not enabled in Market Watch, error code = {mt5.last_error()}") 
        all_symbols = mt5.symbols_get()
        if all_symbols and any(s.name == SYMBOL for s in all_symbols if s is not None): 
            log.info(f"Symbol {SYMBOL} exists but was not selected. Attempting to re-select.")
        else:
            log.warning(f"Symbol {SYMBOL} does not appear to be available on the broker.") 
        mt5.shutdown()
        sys.exit(1)
    
    symbol_info = mt5.symbol_info(SYMBOL)
    if symbol_info is None:
        log.error(f"Failed to get info for symbol {SYMBOL}, error code = {mt5.last_error()}") 
        mt5.shutdown()
        sys.exit(1)
    log.info(f"Symbol {SYMBOL} selected. Description: {symbol_info.description}, Digits: {symbol_info.digits}")

    if initial_equity == 0.0: 
        initial_equity = account_info.equity
        log.info(f"✅ Login successful. Account: {LOGIN}, Initial Equity for profit tracking set to: {initial_equity:.2f} {account_info.currency}")
    else:
        log.info(f"✅ Re-login successful. Account: {LOGIN}, Current Equity: {account_info.equity:.2f} {account_info.currency}. Profit tracking continues from {initial_equity:.2f}.")


def pip_val() -> float:
    info = mt5.symbol_info(SYMBOL)
    if not info:
        log.error(f"Could not get symbol info for {SYMBOL} in pip_val. Defaulting pip value.") 
        return 0.0001 
    
    if info.digits == 5 or info.digits == 4: 
        return 0.0001
    elif info.digits == 3 or info.digits == 2:
        return 0.01
    else: 
        log.warning(f"Uncommon number of digits ({info.digits}) for {SYMBOL}. Using point value as pip value: {info.point}") 
        return info.point 

def tp_price(direction: str, entry_price: float) -> float:
    positions = mt5.positions_get(symbol=SYMBOL) or []
    current_side_type = mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL
    count = sum(1 for p in positions if p.type == current_side_type) 

    pips_to_use = DYNAMIC_PROFIT_PIPS if count >= DYNAMIC_POSITIONS_TRIGGER else PROFIT_PIPS
    price_offset = pips_to_use * pip_val()
    s_info_digits = mt5.symbol_info(SYMBOL).digits

    if direction == "BUY":
        return round(entry_price + price_offset, s_info_digits)
    else: # SELL
        return round(entry_price - price_offset, s_info_digits)

def adjust_lot(lot: float) -> float:
    info = mt5.symbol_info(SYMBOL)
    if not info:
        log.error(f"Could not get symbol info for {SYMBOL} in adjust_lot. Returning original lot.") 
        return lot

    volume_step = getattr(info, "volume_step", 0.01) 
    volume_min = getattr(info, "volume_min", 0.01)   
    volume_max = getattr(info, "volume_max", LOT_MAX) 

    if volume_step <= 0: 
        log.warning(f"Volume step for {SYMBOL} is invalid ({volume_step}). Using 0.01.") 
        volume_step = 0.01

    lot = round(lot / volume_step) * volume_step
    
    lot = max(volume_min, lot)
    lot = min(volume_max, lot) 
    lot = min(LOT_MAX, lot)    

    precision = 0
    if volume_step > 0:
        precision = abs(int(math.log10(volume_step))) if volume_step < 1 else 0
    
    return round(lot, precision)

def format_mt5_comment(action: str) -> str:
    sane_action = "".join(filter(str.isalnum, action))
    if not sane_action:
        sane_action = "Trade" 
    return sane_action[:31]

# --- Trade execution (Market Orders) ---
def send_market_order(direction: str, lot: float, action_comment_str: str) -> bool: 
    mt5_comment = format_mt5_comment(action_comment_str)

    adjusted_lot = adjust_lot(lot)
    symbol_info_vol_min = getattr(mt5.symbol_info(SYMBOL), "volume_min", 0.01)
    if adjusted_lot < symbol_info_vol_min : 
        log.warning(f"Market Order: Adjusted lot {adjusted_lot} is below minimum {symbol_info_vol_min} for {direction} {lot} ({mt5_comment}). Order not sent.") 
        return False

    price = 0.0
    calculated_tp = 0.0 

    for attempt in range(5): 
        tick = mt5.symbol_info_tick(SYMBOL)
        if tick and tick.bid > 0 and tick.ask > 0: 
            price = tick.ask if direction == "BUY" else tick.bid
            calculated_tp = tp_price(direction, price) 
            break
        log.warning(f"Market Order: Attempt {attempt+1}/5: No valid tick for {direction} ({mt5_comment}). Retrying...") 
        time.sleep(0.2)
    else: 
        log.error(f"Market Order: Failed to get valid tick for {direction} ({mt5_comment}). Cannot send.") 
        return False
    
    if price == 0.0:
        log.error(f"Market Order: Price for {SYMBOL} is zero. Cannot send {direction} ({mt5_comment}).") 
        return False

    request = {
        "action": mt5.TRADE_ACTION_DEAL, 
        "symbol": SYMBOL,
        "volume": adjusted_lot,
        "type": mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL,
        "price": price, 
        "tp": calculated_tp, 
        "deviation": 20, 
        "magic": 12345,  
        "comment": mt5_comment, 
        "type_time": mt5.ORDER_TIME_GTC, 
    }
    
    s_info = mt5.symbol_info(SYMBOL) 
    if s_info and hasattr(s_info, 'filling_modes'): 
        allowed_filling_types = s_info.filling_modes
        if mt5.ORDER_FILLING_IOC in allowed_filling_types:
            request["type_filling"] = mt5.ORDER_FILLING_IOC
        elif mt5.ORDER_FILLING_FOK in allowed_filling_types: 
            request["type_filling"] = mt5.ORDER_FILLING_FOK
        elif len(allowed_filling_types) > 0: 
             request["type_filling"] = allowed_filling_types[0] 
    else:
        log.warning(f"Market Order: Could not determine filling modes for {SYMBOL}. Using default.") 

    log.info(f"Sending Market Order: {direction} {adjusted_lot} {SYMBOL} @ Market (Ref: {price:.5f}) TP: {calculated_tp:.5f} Comment: {mt5_comment}")
    result = mt5.order_send(request)

    if result is None:
        log.error(f"Market Order send failed for {direction} {adjusted_lot} ({mt5_comment}). MT5 returned None. Error: {mt5.last_error()}") 
        return False

    if result.retcode == mt5.TRADE_RETCODE_DONE:
        log.info(f"🟢 Market Order: {direction} {result.volume:.2f} @ {result.price:.5f} TP {calculated_tp:.5f} (Ticket: {result.order}, Comment: {mt5_comment}) successfully placed.")
        return True
    else:
        log.error(f"🔴 Market Order send failed for {direction} {adjusted_lot} ({mt5_comment}). Retcode: {result.retcode}, MT5 Comment: {result.comment}, Error: {mt5.last_error()}") 
        return False

# --- Trade execution (Limit Orders) ---
def place_limit_order(direction: str, price_level: float, lot: float, action_comment_str: str = "grid_limit") -> mt5.OrderSendResult | None: 
    mt5_comment = format_mt5_comment(action_comment_str) 

    adjusted_lot = adjust_lot(lot)
    symbol_info_vol_min = getattr(mt5.symbol_info(SYMBOL), "volume_min", 0.01)
    if adjusted_lot < symbol_info_vol_min:
        log.warning(f"Limit Order: Adjusted lot {adjusted_lot} is below minimum {symbol_info_vol_min} for {direction} at {price_level:.5f}. Order not placed.") 
        return None 

    limit_order_tp = tp_price(direction, price_level) 
    order_type = mt5.ORDER_TYPE_BUY_LIMIT if direction == "BUY" else mt5.ORDER_TYPE_SELL_LIMIT

    log.debug(f"Limit Order: Proceeding to place {direction} limit at {price_level:.5f} (strict validation removed).")

    request = {
        "action": mt5.TRADE_ACTION_PENDING, 
        "symbol": SYMBOL,
        "volume": adjusted_lot,
        "type": order_type,
        "price": price_level, 
        "tp": limit_order_tp,
        "sl": 0.0, 
        "magic": 12345,
        "comment": mt5_comment,
        "type_time": mt5.ORDER_TIME_GTC, 
    }

    if PENDING_ORDER_EXPIRATION_MIN > 0:
        expiration_time = int(time.time()) + PENDING_ORDER_EXPIRATION_MIN * 60
        request["type_time"] = mt5.ORDER_TIME_SPECIFIED
        request["expiration"] = expiration_time

    s_info = mt5.symbol_info(SYMBOL) 
    if s_info and hasattr(s_info, 'filling_modes'): 
        allowed_filling_types = s_info.filling_modes
        if mt5.ORDER_FILLING_RETURN in allowed_filling_types: 
             request["type_filling"] = mt5.ORDER_FILLING_RETURN
        elif len(allowed_filling_types) > 0: 
             request["type_filling"] = allowed_filling_types[0] 
    
    log.info(f"Placing Limit Order: {direction} {adjusted_lot} {SYMBOL} @ {price_level:.5f} TP: {limit_order_tp:.5f} Comment: {mt5_comment}")
    result = mt5.order_send(request)

    if result is None:
        log.error(f"Limit Order placement failed (MT5 returned None) for {direction} {adjusted_lot} @ {price_level:.5f} ({mt5_comment}). Error: {mt5.last_error()}") 
        return None 
    
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        current_tick_info = mt5.symbol_info_tick(SYMBOL)
        current_bid = current_tick_info.bid if current_tick_info else "N/A"
        current_ask = current_tick_info.ask if current_tick_info else "N/A"
        log.error(f"🔴 Limit Order placement FAILED for {direction} {adjusted_lot} @ {price_level:.5f} ({mt5_comment}). Retcode: {result.retcode}, MT5 Comment: {result.comment}, Error: {mt5.last_error()}. Current Bid: {current_bid}, Ask: {current_ask}")
    else:
        log.info(f"🟢 Limit Order: {direction} {result.volume:.2f} {SYMBOL} @ {result.price:.5f} TP {limit_order_tp:.5f} (Order Ticket: {result.order}, Comment: {mt5_comment}) successfully placed.")
    
    return result 

def cancel_pending_orders_by_side(direction: str):
    log.info(f"Attempting to cancel all pending {direction} limit orders.")
    order_type_to_cancel = mt5.ORDER_TYPE_BUY_LIMIT if direction == "BUY" else mt5.ORDER_TYPE_SELL_LIMIT
    
    pending_orders = mt5.orders_get(symbol=SYMBOL) or []
    cancelled_count = 0
    for order in pending_orders:
        if order.type == order_type_to_cancel:
            del_request = {"action": mt5.TRADE_ACTION_REMOVE, "order": order.ticket, "symbol": SYMBOL}
            del_res = mt5.order_send(del_request)
            if del_res and del_res.retcode == mt5.TRADE_RETCODE_DONE:
                log.info(f"Cancelled pending {direction} limit order {order.ticket} @ {order.price_open:.5f}")
                cancelled_count += 1
            else:
                err_code = del_res.retcode if del_res else "N/A"
                log.error(f"Failed to cancel pending {direction} limit order {order.ticket}. Error: {mt5.last_error()}, Retcode: {err_code}") 
    if cancelled_count > 0:
        log.info(f"Cancelled {cancelled_count} pending {direction} limit orders.")
    else:
        log.info(f"No pending {direction} limit orders found to cancel.")


def sync_all_tps(direction: str):
    log.debug(f"Attempting to sync TPs for {direction} side.") 
    side_to_sync = mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL
    
    all_positions_of_side = [p for p in (mt5.positions_get(symbol=SYMBOL) or []) if p.type == side_to_sync]

    if not all_positions_of_side:
        log.debug(f"No open {direction} positions to sync TPs for.") 
        return

    try:
        all_positions_of_side.sort(key=lambda p: (p.time_msc if hasattr(p, 'time_msc') and p.time_msc > 0 else p.time, p.ticket), reverse=True)
        newest_position = all_positions_of_side[0]
    except Exception as e: 
        log.error(f"Could not determine newest position for {direction} side for TP sync: {e}") 
        return
        
    if newest_position.tp == 0:
        log.debug(f"Newest {direction} position {newest_position.ticket} (Time: {newest_position.time_msc if hasattr(newest_position, 'time_msc') else newest_position.time}) has no TP. Nothing to sync for this side.") 
        return

    new_tp_level = newest_position.tp
    log.debug(f"TP Sync check for {direction}: Newest position {newest_position.ticket} TP is {new_tp_level:.5f}. Comparing with others.")
    synced_count = 0
    
    mt5_comment_sync = format_mt5_comment(f"sync_tp_to_{newest_position.ticket}")

    for p in all_positions_of_side:
        if p.ticket == newest_position.ticket or math.isclose(p.tp, new_tp_level, abs_tol=mt5.symbol_info(SYMBOL).point * 0.1): 
            continue 

        log.info(f"TP Sync for {direction}: Position {p.ticket} (TP: {p.tp:.5f}) will be synced to {new_tp_level:.5f}")
        modify_request = {
            "action": mt5.TRADE_ACTION_SLTP, 
            "symbol": SYMBOL,
            "position": p.ticket, 
            "tp": new_tp_level,
            "sl": p.sl, 
            "magic": 12345, 
            "comment": mt5_comment_sync
        }
        result = mt5.order_send(modify_request)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(f"🔄 TP synced for {direction} position {p.ticket} to {new_tp_level:.5f}")
            synced_count += 1
        else:
            err_code = result.retcode if result else "N/A"
            last_err = mt5.last_error()
            log.warning(f"sync_all_tps: Failed to sync TP for {direction} position {p.ticket}. Error: {last_err}, Retcode: {err_code}") 
    
    if synced_count > 0:
        log.info(f"✅ TP Sync Summary for {direction}: {synced_count} positions updated to TP {new_tp_level:.5f}")
    elif newest_position: 
        log.debug(f"No TPs needed syncing for {direction} side based on newest position {newest_position.ticket}.") 


# --- Grid recalculation ---
def recalc_grid():
    global next_buy_px, next_sell_px

    positions = mt5.positions_get(symbol=SYMBOL) or [] 
    buy_positions = [p for p in positions if p.type == mt5.ORDER_TYPE_BUY]
    sell_positions = [p for p in positions if p.type == mt5.ORDER_TYPE_SELL]
    s_info_digits = mt5.symbol_info(SYMBOL).digits

    if buy_positions:
        num_for_dyn_trigger_buy = len(buy_positions) 
        current_grid_step_pips_buy = DYNAMIC_STEP_PIPS if num_for_dyn_trigger_buy >= DYNAMIC_POSITIONS_TRIGGER else GRID_STEP_PIPS
        step_in_price_buy = current_grid_step_pips_buy * pip_val()
        min_buy_price = min(p.price_open for p in buy_positions)
        next_buy_px = round(min_buy_price - step_in_price_buy, s_info_digits)
        log.debug(f"Recalc: Next BUY LIMIT target calculated: {next_buy_px:.{s_info_digits}f}") 
    else: 
        next_buy_px = None 
        log.debug("Recalc: No open buy positions, next_buy_px is None.") 

    if sell_positions:
        num_for_dyn_trigger_sell = len(sell_positions)
        current_grid_step_pips_sell = DYNAMIC_STEP_PIPS if num_for_dyn_trigger_sell >= DYNAMIC_POSITIONS_TRIGGER else GRID_STEP_PIPS
        step_in_price_sell = current_grid_step_pips_sell * pip_val()
        max_sell_price = max(p.price_open for p in sell_positions)
        next_sell_px = round(max_sell_price + step_in_price_sell, s_info_digits)
        log.debug(f"Recalc: Next SELL LIMIT target calculated: {next_sell_px:.{s_info_digits}f}") 
    else: 
        next_sell_px = None
        log.debug("Recalc: No open sell positions, next_sell_px is None.") 

    if next_buy_px is not None and next_sell_px is not None:
         log.info(f"📐 Grid levels after recalc: Next BUY ≤ {next_buy_px:.{s_info_digits}f}, Next SELL ≥ {next_sell_px:.{s_info_digits}f}")
    elif next_buy_px is None and next_sell_px is None:
        log.info("📐 Grid levels are currently None (no open positions on either side).")
    elif next_buy_px is None and next_sell_px is not None: 
        log.info(f"📐 Grid levels after recalc: No BUY target, Next SELL ≥ {next_sell_px:.{s_info_digits}f}")
    elif next_sell_px is None and next_buy_px is not None: 
        log.info(f"📐 Grid levels after recalc: Next BUY ≤ {next_buy_px:.{s_info_digits}f}, No SELL target")


# --- Auto-hedge handling (using Market Orders) ---
def hedge_if_empty():
    global last_buy_lot, last_sell_lot, prev_buy_count, prev_sell_count
    global buy_fib_index, sell_fib_index, just_hedged 

    positions = mt5.positions_get(symbol=SYMBOL) or []
    if positions: 
        return

    log.info("🔄 No positions found. Initiating initial hedge (Market Orders)...")
    last_buy_lot = LOT_SMALL
    last_sell_lot = LOT_SMALL
    buy_fib_index = 1 
    sell_fib_index = 1 

    buy_success = send_market_order("BUY", LOT_SMALL, action_comment_str="initial_hedge_buy")
    sell_success = send_market_order("SELL", LOT_SMALL, action_comment_str="initial_hedge_sell")

    if buy_success and sell_success:
        log.info("Initial market hedge orders placed successfully.")
        recalc_grid() 
        current_positions_after_hedge = mt5.positions_get(symbol=SYMBOL) or []
        prev_buy_count = sum(1 for p in current_positions_after_hedge if p.type == mt5.ORDER_TYPE_BUY)
        prev_sell_count = sum(1 for p in current_positions_after_hedge if p.type == mt5.ORDER_TYPE_SELL)
        just_hedged = True 
        log.info(f"Initial hedge complete. Buy count: {prev_buy_count}, Sell count: {prev_sell_count}. Pausing for OPEN_DELAY.")
        time.sleep(OPEN_DELAY) 
    else:
        log.error("Failed to place one or both initial market hedge orders.") 
        

# --- Closed hedge detection (using Market Orders) ---
def handle_closed_hedge():
    global prev_buy_count, prev_sell_count, last_buy_lot, last_sell_lot
    global buy_fib_index, sell_fib_index, just_hedged, next_buy_px, next_sell_px

    current_positions = mt5.positions_get(symbol=SYMBOL) or []
    current_buy_count = sum(1 for p in current_positions if p.type == mt5.ORDER_TYPE_BUY)
    current_sell_count = sum(1 for p in current_positions if p.type == mt5.ORDER_TYPE_SELL)

    side_reset_this_cycle = False

    if current_buy_count < prev_buy_count: 
        log.info(f"🔔 BUY position(s) closed. Prev: {prev_buy_count}, Current: {current_buy_count}. Resetting BUY side.")
        cancel_pending_orders_by_side("BUY")
        last_buy_lot = LOT_SMALL
        buy_fib_index = 1 
        next_buy_px = None 
        side_reset_this_cycle = True
        if current_buy_count == 0 and current_sell_count > 0 : 
            log.info("BUY side empty, Sells exist. Re-hedging BUY with market order.")
            if send_market_order("BUY", LOT_SMALL, action_comment_str="re_hedge_buy_after_closure"):
                current_buy_count = 1 
                just_hedged = True 
            else:
                log.error("Failed to re-hedge BUY side after closure.") 
        recalc_grid() 

    if current_sell_count < prev_sell_count: 
        log.info(f"🔔 SELL position(s) closed. Prev: {prev_sell_count}, Current: {current_sell_count}. Resetting SELL side.")
        cancel_pending_orders_by_side("SELL")
        last_sell_lot = LOT_SMALL
        sell_fib_index = 1 
        next_sell_px = None 
        side_reset_this_cycle = True
        if current_sell_count == 0 and current_buy_count > 0 : 
            log.info("SELL side empty, Buys exist. Re-hedging SELL with market order.")
            if send_market_order("SELL", LOT_SMALL, action_comment_str="re_hedge_sell_after_closure"):
                current_sell_count = 1 
                just_hedged = True 
            else:
                log.error("Failed to re-hedge SELL side after closure.") 
        recalc_grid() 

    prev_buy_count = current_buy_count
    prev_sell_count = current_sell_count

    if side_reset_this_cycle and not just_hedged: 
        pass
    elif just_hedged: 
        log.info(f"Re-hedge (market order) occurred due to side closure. Pausing for OPEN_DELAY.")
        time.sleep(OPEN_DELAY)


# --- Grid stepping logic (using Limit Orders) ---
def step_grid(): 
    global last_buy_lot, last_sell_lot, buy_fib_index, sell_fib_index
    global next_buy_px, next_sell_px, just_hedged

    if just_hedged: 
        log.debug("step_grid: Skipping grid step due to recent hedge operation.") 
        return

    all_positions = mt5.positions_get(symbol=SYMBOL) or []
    if len(all_positions) >= MAX_POSITIONS: 
        log.warning(f"Max open positions ({MAX_POSITIONS}) reached. No new grid limit orders will be placed.") 
        return

    # Attempt to establish grid levels if they are currently None
    if next_buy_px is None and next_sell_px is None and not all_positions:
        log.debug("step_grid: Both next_buy_px and next_sell_px are None, and no open positions. Likely waiting for initial hedge.")
        return 
    elif next_buy_px is None or next_sell_px is None: 
        log.debug("step_grid: One side of grid levels is None. Attempting recalc_grid.")
        recalc_grid()
        # Check again after recalc
        if next_buy_px is None and next_sell_px is None and not all_positions:
             log.debug("step_grid: Grid levels still None after recalc and no positions. Waiting for hedge.")
             return


    buy_positions_count = sum(1 for p in all_positions if p.type == mt5.ORDER_TYPE_BUY)
    sell_positions_count = sum(1 for p in all_positions if p.type == mt5.ORDER_TYPE_SELL)
    
    s_info = mt5.symbol_info(SYMBOL)
    if not s_info:
        log.error("step_grid: Could not get symbol info. Cannot proceed.") 
        return
    symbol_point = s_info.point
    s_info_digits = s_info.digits
    abs_tolerance_for_isclose = symbol_point * 2.0 

    pending_orders = mt5.orders_get(symbol=SYMBOL) or []
    current_pending_buy_limits = sum(1 for o in pending_orders if o.type == mt5.ORDER_TYPE_BUY_LIMIT)
    current_pending_sell_limits = sum(1 for o in pending_orders if o.type == mt5.ORDER_TYPE_SELL_LIMIT)

    # --- BUY LIMIT ---
    log.debug(f"step_grid: Considering BUY_LIMIT. next_buy_px: {next_buy_px}, Pending Buys: {current_pending_buy_limits}, Max: {MAX_PENDING_GRID_ORDERS_PER_SIDE}, Buy Fib Index: {buy_fib_index}")
    if next_buy_px is None:
        log.info("step_grid: next_buy_px is None. Skipping BUY_LIMIT placement attempt this cycle.")
    elif current_pending_buy_limits >= MAX_PENDING_GRID_ORDERS_PER_SIDE:
        log.debug(f"step_grid: Max pending BUY_LIMIT orders ({MAX_PENDING_GRID_ORDERS_PER_SIDE}) reached. Not placing new buy limit.")
    else:
        can_place_buy_limit = True
        for order in pending_orders: 
            if order.type == mt5.ORDER_TYPE_BUY_LIMIT and math.isclose(order.price_open, next_buy_px, abs_tol=abs_tolerance_for_isclose):
                log.debug(f"step_grid: Existing BUY_LIMIT order {order.ticket} found at {order.price_open:.{s_info_digits}f}, near target {next_buy_px:.{s_info_digits}f}. Not placing another.") 
                can_place_buy_limit = False
                break
        
        if can_place_buy_limit:
            target_buy_price_for_this_order = next_buy_px 
            log.info(f"step_grid: Conditions met to place BUY_LIMIT @ {target_buy_price_for_this_order:.{s_info_digits}f}")
            temp_buy_fib_index = buy_fib_index + 1 
            
            if temp_buy_fib_index <= FIB_LIMIT:
                raw_lot = LOT_SMALL * fib(temp_buy_fib_index)
                log.info(f"BUY_LIMIT using Fibonacci: next index {temp_buy_fib_index}, fib_val {fib(temp_buy_fib_index)}, raw_lot {raw_lot:.4f}")
            else:
                raw_lot = last_buy_lot * DYNAMIC_MULTIPLIER
                log.info(f"BUY_LIMIT using Dynamic Multiplier: last_buy_lot {last_buy_lot:.4f}, multiplier {DYNAMIC_MULTIPLIER}, raw_lot {raw_lot:.4f}")
            
            current_buy_lot = adjust_lot(raw_lot) 
            
            order_result = place_limit_order("BUY", target_buy_price_for_this_order, current_buy_lot, action_comment_str="grid_buy_limit")
            if order_result and order_result.retcode == mt5.TRADE_RETCODE_DONE:
                buy_fib_index = temp_buy_fib_index 
                last_buy_lot = current_buy_lot 
                
                current_gap_pips_buy = DYNAMIC_STEP_PIPS if (buy_positions_count + 1) >= DYNAMIC_POSITIONS_TRIGGER else GRID_STEP_PIPS
                gap_in_price_buy = current_gap_pips_buy * pip_val()
                
                next_buy_px = round(target_buy_price_for_this_order - gap_in_price_buy, s_info_digits) 
                log.info(f"BUY_LIMIT placed. Global next_buy_px advanced to: {next_buy_px:.{s_info_digits}f}")
                
                time.sleep(OPEN_DELAY) 
            else:
                log.info("Attempt to place BUY_LIMIT order was not successful (see previous logs for reason). Fibonacci index not incremented.")
                if order_result and order_result.retcode == 10015: # TRADE_RETCODE_INVALID_PRICE
                    log.warning(f"BUY_LIMIT failed for {target_buy_price_for_this_order:.{s_info_digits}f} due to Invalid Price (Retcode: 10015). Advancing to next potential grid level.")
                    current_gap_pips_buy = DYNAMIC_STEP_PIPS if (buy_positions_count) >= DYNAMIC_POSITIONS_TRIGGER else GRID_STEP_PIPS 
                    gap_in_price_buy = current_gap_pips_buy * pip_val()
                    next_buy_px = round(target_buy_price_for_this_order - gap_in_price_buy, s_info_digits) 
                    log.info(f"Invalid price skip: Global next_buy_px (target for next attempt) advanced to: {next_buy_px:.{s_info_digits}f}")
        # else: (already logged if existing order found)
    
    # --- SELL LIMIT ---
    log.debug(f"step_grid: Considering SELL_LIMIT. next_sell_px: {next_sell_px}, Pending Sells: {current_pending_sell_limits}, Max: {MAX_PENDING_GRID_ORDERS_PER_SIDE}, Sell Fib Index: {sell_fib_index}")
    if next_sell_px is None:
        log.info("step_grid: next_sell_px is None. Skipping SELL_LIMIT placement attempt this cycle.")
    elif current_pending_sell_limits >= MAX_PENDING_GRID_ORDERS_PER_SIDE:
        log.debug(f"step_grid: Max pending SELL_LIMIT orders ({MAX_PENDING_GRID_ORDERS_PER_SIDE}) reached. Not placing new sell limit.")
    else:
        can_place_sell_limit = True
        for order in pending_orders: 
            if order.type == mt5.ORDER_TYPE_SELL_LIMIT and math.isclose(order.price_open, next_sell_px, abs_tol=abs_tolerance_for_isclose):
                log.debug(f"step_grid: Existing SELL_LIMIT order {order.ticket} found at {order.price_open:.{s_info_digits}f}, near target {next_sell_px:.{s_info_digits}f}. Not placing another.") 
                can_place_sell_limit = False
                break

        if can_place_sell_limit:
            target_sell_price_for_this_order = next_sell_px 
            log.info(f"step_grid: Conditions met to place SELL_LIMIT @ {target_sell_price_for_this_order:.{s_info_digits}f}")
            temp_sell_fib_index = sell_fib_index + 1 

            if temp_sell_fib_index <= FIB_LIMIT:
                raw_lot = LOT_SMALL * fib(temp_sell_fib_index)
                log.info(f"SELL_LIMIT using Fibonacci: next index {temp_sell_fib_index}, fib_val {fib(temp_sell_fib_index)}, raw_lot {raw_lot:.4f}")
            else:
                raw_lot = last_sell_lot * DYNAMIC_MULTIPLIER
                log.info(f"SELL_LIMIT using Dynamic Multiplier: last_sell_lot {last_sell_lot:.4f}, multiplier {DYNAMIC_MULTIPLIER}, raw_lot {raw_lot:.4f}")

            current_sell_lot = adjust_lot(raw_lot)

            order_result = place_limit_order("SELL", target_sell_price_for_this_order, current_sell_lot, action_comment_str="grid_sell_limit")
            if order_result and order_result.retcode == mt5.TRADE_RETCODE_DONE:
                sell_fib_index = temp_sell_fib_index 
                last_sell_lot = current_sell_lot 
                
                current_gap_pips_sell = DYNAMIC_STEP_PIPS if (sell_positions_count + 1) >= DYNAMIC_POSITIONS_TRIGGER else GRID_STEP_PIPS
                gap_in_price_sell = current_gap_pips_sell * pip_val()

                next_sell_px = round(target_sell_price_for_this_order + gap_in_price_sell, s_info_digits)
                log.info(f"SELL_LIMIT placed. Global next_sell_px advanced to: {next_sell_px:.{s_info_digits}f}")
                
                time.sleep(OPEN_DELAY) 
            else:
                log.info("Attempt to place SELL_LIMIT order was not successful (see previous logs for reason). Fibonacci index not incremented.")
                if order_result and order_result.retcode == 10015: # TRADE_RETCODE_INVALID_PRICE
                    log.warning(f"SELL_LIMIT failed for {target_sell_price_for_this_order:.{s_info_digits}f} due to Invalid Price (Retcode: 10015). Advancing to next potential grid level.")
                    current_gap_pips_sell = DYNAMIC_STEP_PIPS if (sell_positions_count) >= DYNAMIC_POSITIONS_TRIGGER else GRID_STEP_PIPS
                    gap_in_price_sell = current_gap_pips_sell * pip_val()
                    next_sell_px = round(target_sell_price_for_this_order + gap_in_price_sell, s_info_digits)
                    log.info(f"Invalid price skip: Global next_sell_px (target for next attempt) advanced to: {next_sell_px:.{s_info_digits}f}")
        # else: (already logged if existing order found)

# --- Main execution loop ---
def run():
    global initial_equity, prev_buy_count, prev_sell_count, just_hedged
    global last_buy_lot, last_sell_lot, buy_fib_index, sell_fib_index, next_buy_px, next_sell_px
    global loop_counter 

    mt5_login() 

    existing_positions = mt5.positions_get(symbol=SYMBOL) or []
    prev_buy_count = sum(1 for p in existing_positions if p.type == mt5.ORDER_TYPE_BUY)
    prev_sell_count = sum(1 for p in existing_positions if p.type == mt5.ORDER_TYPE_SELL)

    symbol_info_data = mt5.symbol_info(SYMBOL)
    volume_step_tolerance = LOT_SMALL * 0.01 
    if symbol_info_data and hasattr(symbol_info_data, 'volume_step') and symbol_info_data.volume_step > 0:
        volume_step_tolerance = symbol_info_data.volume_step / 2.0
    else:
        log.warning(f"Could not get valid volume_step for {SYMBOL} for tolerance calc. Using fallback: {volume_step_tolerance}") 


    if existing_positions:
        log.info(f"↻ Restarted. Found {len(existing_positions)} existing positions. Buys: {prev_buy_count}, Sells: {prev_sell_count}.")
        
        current_max_buy_lot = 0.0
        if prev_buy_count > 0:
            buy_lots_volumes = [p.volume for p in existing_positions if p.type == mt5.ORDER_TYPE_BUY]
            current_max_buy_lot = max(buy_lots_volumes) if buy_lots_volumes else 0.0
        
        current_max_sell_lot = 0.0
        if prev_sell_count > 0:
            sell_lots_volumes = [p.volume for p in existing_positions if p.type == mt5.ORDER_TYPE_SELL]
            current_max_sell_lot = max(sell_lots_volumes) if sell_lots_volumes else 0.0

        last_buy_lot = current_max_buy_lot if current_max_buy_lot > 0 else LOT_SMALL
        last_sell_lot = current_max_sell_lot if current_max_sell_lot > 0 else LOT_SMALL
        log.info(f"Restart: Initial last_buy_lot set to {last_buy_lot:.4f}, last_sell_lot to {last_sell_lot:.4f}")

        buy_fib_index = 0
        sell_fib_index = 0

        if prev_buy_count > 0:
            matched_fib_buy_index = 0
            for i in range(1, FIB_LIMIT + 1):
                expected_fib_lot = adjust_lot(LOT_SMALL * fib(i))
                if abs(current_max_buy_lot - expected_fib_lot) <= volume_step_tolerance:
                    buy_fib_index = i 
                    matched_fib_buy_index = i
                    log.info(f"Restart: Buy side matched Fibonacci. Max existing buy lot: {current_max_buy_lot:.4f}, matched fib({i}) lot: {expected_fib_lot:.4f}. Setting buy_fib_index to {i}.")
                    break
            if matched_fib_buy_index == 0: 
                if current_max_buy_lot > adjust_lot(LOT_SMALL * fib(FIB_LIMIT)): 
                    buy_fib_index = FIB_LIMIT 
                    log.info(f"Restart: Buy side, no Fibonacci match. Max existing buy lot {current_max_buy_lot:.4f} > max Fib lot. Setting buy_fib_index to {FIB_LIMIT} (for dynamic phase next).")
                else: 
                    log.info(f"Restart: Buy side, no Fibonacci match and not clearly in dynamic phase. Max lot {current_max_buy_lot:.4f}. buy_fib_index at 0 (will be 1 for next trade).")
        else: 
            last_buy_lot = LOT_SMALL 
            log.info("Restart: No existing buy positions. Setting buy_fib_index to 0, last_buy_lot to LOT_SMALL.")

        if prev_sell_count > 0:
            matched_fib_sell_index = 0
            for i in range(1, FIB_LIMIT + 1):
                expected_fib_lot = adjust_lot(LOT_SMALL * fib(i))
                if abs(current_max_sell_lot - expected_fib_lot) <= volume_step_tolerance:
                    sell_fib_index = i
                    matched_fib_sell_index = i
                    log.info(f"Restart: Sell side matched Fibonacci. Max existing sell lot: {current_max_sell_lot:.4f}, matched fib({i}) lot: {expected_fib_lot:.4f}. Setting sell_fib_index to {i}.")
                    break
            if matched_fib_sell_index == 0: 
                if current_max_sell_lot > adjust_lot(LOT_SMALL * fib(FIB_LIMIT)): 
                    sell_fib_index = FIB_LIMIT
                    log.info(f"Restart: Sell side, no Fibonacci match. Max existing sell lot {current_max_sell_lot:.4f} > max Fib lot. Setting sell_fib_index to {FIB_LIMIT} (for dynamic phase next).")
                else: 
                    log.info(f"Restart: Sell side, no Fibonacci match and not clearly in dynamic phase. Max lot {current_max_sell_lot:.4f}. sell_fib_index at 0 (will be 1 for next trade).")
        else: 
            last_sell_lot = LOT_SMALL
            log.info("Restart: No existing sell positions. Setting sell_fib_index to 0, last_sell_lot to LOT_SMALL.")
        
        recalc_grid() 
    else:
        log.info("No existing positions. Bot will start by hedging.")
        buy_fib_index = 0 
        sell_fib_index = 0
        last_buy_lot = LOT_SMALL
        last_sell_lot = LOT_SMALL
        next_buy_px = None 
        next_sell_px = None
        hedge_if_empty() 

    try:
        while True:
            loop_counter += 1
            if mt5.terminal_info() is None: 
                log.error("Lost connection to MetaTrader 5 terminal. Attempting to reconnect...") 
                mt5.shutdown()
                time.sleep(10) 
                mt5_login() 
                if mt5.terminal_info() is None:
                    log.error("Failed to reconnect. Exiting.") 
                    break 
            
            if loop_counter % LOG_BALANCE_INTERVAL == 0:
                acc_info = mt5.account_info()
                if acc_info:
                    log.info(f"Account Status - Balance: {acc_info.balance:.2f} {acc_info.currency}, Equity: {acc_info.equity:.2f} {acc_info.currency}")

            if PROFIT_TARGET_AMT > 0:
                account_info = mt5.account_info()
                if account_info:
                    current_profit = account_info.equity - initial_equity
                    if current_profit >= PROFIT_TARGET_AMT:
                        log.warning(f"🎯 PROFIT TARGET of {PROFIT_TARGET_AMT:.2f} REACHED! Current profit: {current_profit:.2f}. Equity: {account_info.equity:.2f}") 
                        log.warning(f"Closing all positions and pending orders for symbol {SYMBOL}...") 
                        
                        cancel_pending_orders_by_side("BUY") 
                        cancel_pending_orders_by_side("SELL") 
                        log.info(f"Profit Target: Cancelled all pending grid orders.")

                        open_positions = mt5.positions_get(symbol=SYMBOL) or []
                        closed_positions_count = 0
                        for position in open_positions:
                            close_direction = mt5.ORDER_TYPE_SELL if position.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
                            close_tick = mt5.symbol_info_tick(SYMBOL)
                            if not close_tick:
                                log.error(f"Could not get tick to close position {position.ticket}. Skipping.") 
                                continue
                            close_price = close_tick.bid if position.type == mt5.ORDER_TYPE_BUY else close_tick.ask
                            if close_price == 0:
                                log.error(f"Invalid close price (0) for position {position.ticket}. Skipping.") 
                                continue

                            close_request = {
                                "action": mt5.TRADE_ACTION_DEAL, "symbol": SYMBOL,
                                "volume": position.volume, "type": close_direction,
                                "position": position.ticket, "price": close_price,
                                "deviation": 30, "magic": 12345, 
                                "comment": format_mt5_comment("profit_target_close")
                            }
                            result = mt5.order_send(close_request)
                            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                                log.info(f"Closed position {position.ticket} for profit target.")
                                closed_positions_count +=1
                            else:
                                log.error(f"Failed to close position {position.ticket}. Error: {mt5.last_error()}, Retcode: {result.retcode if result else 'N/A'}") 
                        
                        log.info(f"Profit target: Closed {closed_positions_count}/{len(open_positions)} open positions.")
                        
                        log.info("Resetting bot state for new cycle...")
                        last_buy_lot = LOT_SMALL; last_sell_lot = LOT_SMALL
                        buy_fib_index = 0; sell_fib_index = 0 
                        next_buy_px = None; next_sell_px = None
                        
                        new_account_info = mt5.account_info() 
                        if new_account_info:
                            initial_equity = new_account_info.equity
                            log.info(f"New initial_equity for profit tracking: {initial_equity:.2f}")
                        else:
                            log.error("Could not get account info to update initial_equity after profit target reset!") 

                        hedge_if_empty() 
                        log.info("Bot reset complete due to profit target. Continuing operation.")
                        continue 

            handle_closed_hedge() 
            hedge_if_empty()      
            if just_hedged:
                log.debug("Main loop: just_hedged is true, resetting and continuing.") 
                just_hedged = False 
            else:
                step_grid() 
            
            sync_all_tps("BUY")
            sync_all_tps("SELL")
            
            time.sleep(LOOP_MS / 1000.0) 

    except KeyboardInterrupt:
        log.info("User requested shutdown (KeyboardInterrupt).")
    except Exception as e:
        log.error(f"An unexpected error occurred in the main loop: {e}") 
        log.error(traceback.format_exc()) 
    finally:
        log.info("Shutting down MetaTrader 5 connection...")
        cancel_pending_orders_by_side("BUY")
        cancel_pending_orders_by_side("SELL")
        mt5.shutdown()
        log.info("Bot stopped.")

if __name__ == '__main__':
    log.info(f"Starting {BOT_NAME} - Grid Martingale Bot (v103.4-USER-BASE-REACTIVE-INVALID-PRICE)") 
    log.info(f"SYMBOL: {SYMBOL}, LOT_SMALL: {LOT_SMALL}, FIB_LIMIT: {FIB_LIMIT}, DYN_MULTIPLIER: {DYNAMIC_MULTIPLIER}")
    log.info(f"GRID_PIPS: {GRID_STEP_PIPS}, DYN_GRID_PIPS: {DYNAMIC_STEP_PIPS}, PROFIT_PIPS: {PROFIT_PIPS}, DYN_PROFIT_PIPS: {DYNAMIC_PROFIT_PIPS}")
    log.info(f"MAX_POS: {MAX_POSITIONS}, OPEN_DELAY: {OPEN_DELAY}s, DYN_TRIGGER: {DYNAMIC_POSITIONS_TRIGGER}, PROFIT_TARGET: {PROFIT_TARGET_AMT}")
    log.info(f"PENDING_ORDER_EXP_MIN: {PENDING_ORDER_EXPIRATION_MIN}, MAX_PENDING_GRID_ORDERS_PER_SIDE: {MAX_PENDING_GRID_ORDERS_PER_SIDE}")
    log.info(f"LOG_BALANCE_INTERVAL: {LOG_BALANCE_INTERVAL} loops")
    run()
# Ensure no invalid characters at the very end of the file.
