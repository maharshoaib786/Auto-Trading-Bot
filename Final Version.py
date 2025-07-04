# mt5_grid_martingale.py (v110.11-StaticGrid-Refined)
"""
MT5 Grid Bot v110.11-StaticGrid-Refined
=======================================

A sophisticated grid trading bot for MetaTrader 5 that employs a static grid system for
predictable trade placement and manages multiple independent grids.

Key Features:
-------------
- **Static Grid Anchoring**:
  - The price of the first trade on each side (BUY/SELL) acts as a fixed "anchor".
  - All subsequent grid levels are calculated as precise offsets from this anchor,
    ensuring consistent `GRID_STEP_PIPS` spacing, immune to market gaps or partial fills.

- **Multi-Order Grid Population**:
  - The bot actively maintains a configurable number of pending limit orders
    (defined by `MAX_PENDING_GRID_ORDERS_PER_SIDE`) on each side of the grid.
  - As orders are filled, new ones are automatically placed for the next level in the
    sequence, keeping the grid well-populated.

- **Dynamic Capping & Multi-Grid Hedging**:
  - When a grid side reaches the `DYNAMIC_POSITIONS_TRIGGER` limit, that grid is "frozen".
  - An internal counter-hedge is placed within the frozen grid.
  - If `MAX_ACTIVE_GRIDS` allows, a completely new, independent grid is started with its
    own hedge and anchor prices.

- **Resilient Grid Resets**:
  - A grid's anchor remains active as long as positions exist on that side.
  - If all positions on one side close (e.g., all BUYs), that side is reset. The bot
    places a new "re-hedge" market order, and its price becomes the new anchor for a
    fresh static grid on that side.

- **Robust Restart Logic**:
  - On startup, the bot intelligently reconstructs the state of all active grids,
    including anchor prices and sequence progress, from existing open positions and
    pending orders.

- **Configuration**:
  - All parameters are managed through an external `.env` file for easy adjustments.
"""
import os, sys, time, math, logging, traceback, datetime
import MetaTrader5 as mt5
from dotenv import load_dotenv

# Ensure UTF-8 encoding on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()


# --- Configuration ---
# --- Configuration ---
SYMBOL                    = os.getenv("SYMBOL", "XAUUSDc")
LOT_SMALL                 = float(os.getenv("LOT_SMALL", 0.01))
LOT_MAX                   = float(os.getenv("LOT_MAX", 20))
GRID_LOT_SEQUENCE         = [0.01, 0.01, 0.02, 0.03, 0.03, 0.04, 0.06, 0.08, 0.10, 0.14, 0.18, 0.24, 0.31, 0.41, 0.55, 0.72, 0.95, 1.26, 1.67, 2.2, 2.91, 3.85, 5.09, 6.72, 8.89, 11.75, 15.53, 20.0]
GRID_STEP_PIPS            = float(os.getenv("GRID_STEP_PIPS", 100))
DYNAMIC_STEP_PIPS         = float(os.getenv("DYNAMIC_STEP_PIPS", 120))
PROFIT_PIPS               = float(os.getenv("PROFIT_PIPS", 350))
DYNAMIC_PROFIT_PIPS       = float(os.getenv("DYNAMIC_PROFIT_PIPS", 450))
DYNAMIC_POSITIONS_TRIGGER = int(os.getenv("DYNAMIC_POSITIONS_TRIGGER", 25))
MAX_POSITIONS             = int(os.getenv("MAX_POS", 60)) # Max total open positions across ALL grids
PROFIT_TARGET_AMT         = float(os.getenv("PROFIT_TARGET_AMT", 1000)) 
OPEN_DELAY                = float(os.getenv("OPEN_DELAY", 1)) 
LOOP_MS                   = int(os.getenv("LOOP_MS", 100))
LOG_FILE                  = os.getenv("LOG_FILE", "grid_bot.log")
LOGIN                     = int(os.getenv("MT5_LOGIN", 0))
PASSWORD                  = os.getenv("MT5_PASSWORD", "")
SERVER                    = os.getenv("MT5_SERVER", "")
PENDING_ORDER_EXPIRATION_MIN = int(os.getenv("PENDING_ORDER_EXPIRATION_MIN", 0))
MAX_PENDING_GRID_ORDERS_PER_SIDE = int(os.getenv("MAX_PENDING_GRID_ORDERS_PER_SIDE", 2))
LOG_BALANCE_INTERVAL      = int(os.getenv("LOG_BALANCE_INTERVAL", 100))
MAX_ACTIVE_GRIDS          = int(os.getenv("MAX_ACTIVE_GRIDS", 2)) # Limit for concurrent grids
INVALID_PRICE_RETRY_LIMIT = int(os.getenv("INVALID_PRICE_RETRY_LIMIT", 10))
SLIPPAGE                  = int(os.getenv("SLIPPAGE", 20)) # Slippage for market orders
INVALID_PRICE_ADJUST_PIPS = float(os.getenv("INVALID_PRICE_ADJUST_PIPS", 100.0)) # Pips to adjust price by on invalid price retry
ENABLE_TRADING_HOURS      = os.getenv("ENABLE_TRADING_HOURS", "False").lower() == "true"
TRADING_START_TIME_STR    = os.getenv("TRADING_START_TIME", "00:00") # Expected in HH:MM format, UTC
TRADING_END_TIME_STR      = os.getenv("TRADING_END_TIME", "23:59")   # Expected in HH:MM format, UTC
MAX_LOSS_AMT              = float(os.getenv("MAX_LOSS_AMT", "0")) # Max allowable loss from initial_equity for current cycle. 0 or less disables.

# --- Action Comment Strings & Search Keywords ---
# These constants centralize all comment strings, making the code cleaner and less prone to typos.
# They are passed as 'action_comment_str' and then processed by format_mt5_comment.
ACTION_INITIAL_HEDGE_BUY = "initial_hedge_buy"
ACTION_INITIAL_HEDGE_SELL = "initial_hedge_sell"
ACTION_RE_HEDGE_BUY = "re_hedge_buy"
ACTION_RE_HEDGE_SELL = "re_hedge_sell"
ACTION_GRID_LIMIT_BUY = "gridbuy"
ACTION_GRID_LIMIT_SELL = "gridsell"
ACTION_GRID_BUY_MARKET_FALLBACK = "grid_buy_market_fallback"
ACTION_GRID_SELL_MARKET_FALLBACK = "grid_sell_market_fallback"
ACTION_CAPPED_BUY_TAG = "CappedBuy"
ACTION_CAPPED_SELL_TAG = "CappedSell"
ACTION_DYNAMIC_HEDGE_BUY_TAG = "DynamicHedgeBuy"
ACTION_DYNAMIC_HEDGE_SELL_TAG = "DynamicHedgeSell"

# Suffixes for system-generated close comments
ACTION_SUFFIX_PROFIT_TARGET_CLOSE = "profit_target_close"
ACTION_SUFFIX_MAX_LOSS_CLOSE = "max_loss_limit_close"

# Grid name for system-level comments (like profit/loss target closures)
SYSTEM_COMMENT_GRID_NAME = "System"

# Keywords used for searching within existing position comments
SEARCH_KEYWORD_CAPPED_GENERIC = "Capped"
SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC = "DynamicHedge"
SEARCH_KEYWORD_CAPPED_BUY_SPECIFIC = ACTION_CAPPED_BUY_TAG
SEARCH_KEYWORD_CAPPED_SELL_SPECIFIC = ACTION_CAPPED_SELL_TAG
SEARCH_KEYWORD_GRIDBUY_SANE = "gridbuy"
SEARCH_KEYWORD_GRIDSELL_SANE = "gridsell"
SEARCH_KEYWORD_MARKET_FALLBACK_SUBSTRING = "market_fallback"

# --- Logging setup ---
log = logging.getLogger("grid_bot")
log.setLevel(logging.INFO)

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

# --- Parsed Configuration & Global State for Trading Hours ---
TRADING_START_TIME_OBJ: datetime.time | None = None
TRADING_END_TIME_OBJ: datetime.time | None = None

if ENABLE_TRADING_HOURS:
    try:
        TRADING_START_TIME_OBJ = datetime.datetime.strptime(TRADING_START_TIME_STR, "%H:%M").time()
        TRADING_END_TIME_OBJ = datetime.datetime.strptime(TRADING_END_TIME_STR, "%H:%M").time()
    except ValueError:
        log.error(f"Invalid TRADING_START_TIME ('{TRADING_START_TIME_STR}') or TRADING_END_TIME ('{TRADING_END_TIME_STR}') format. Please use HH:MM. Trading hours feature disabled.")
        ENABLE_TRADING_HOURS = False
    except NameError:
        log.error("Datetime module not available for parsing trading hours. Trading hours feature disabled.")
        ENABLE_TRADING_HOURS = False


# --- State variables ---
initial_equity: float      = 0.0
grid_states = {}
base_magic_number = 12345
next_magic_number = 12346
loop_counter: int          = 0

# --- Trading Hours Utility ---
def is_trading_session_active() -> bool:
    """Checks if the current time is within the configured trading hours."""
    if not ENABLE_TRADING_HOURS or TRADING_START_TIME_OBJ is None or TRADING_END_TIME_OBJ is None:
        return True

    terminal_info_val = mt5.terminal_info()
    if terminal_info_val is None or not hasattr(terminal_info_val, 'time'):
        log.warning("is_trading_session_active: Could not get terminal info or server time. Assuming trading is active to be safe.")
        return True

    server_datetime_utc = datetime.datetime.fromtimestamp(terminal_info_val.time, tz=datetime.timezone.utc)
    current_time_utc = server_datetime_utc.time()

    if TRADING_START_TIME_OBJ <= TRADING_END_TIME_OBJ:
        is_active = TRADING_START_TIME_OBJ <= current_time_utc < TRADING_END_TIME_OBJ
    else: # Overnight case
        is_active = current_time_utc >= TRADING_START_TIME_OBJ or current_time_utc < TRADING_END_TIME_OBJ
    return is_active

# --- Utilities ---
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
        log.critical(f"CRITICAL: Could not get symbol info for {SYMBOL} in pip_val. Cannot determine pip value. Exiting.")
        raise ValueError(f"Failed to get symbol info for {SYMBOL}, cannot determine pip_val.")

    if info.digits in [5, 4]: return 0.0001
    if info.digits in [3, 2]: return 0.01
    log.warning(f"Uncommon number of digits ({info.digits}) for {SYMBOL}. Using point value as pip value: {info.point}")
    return info.point

def tp_price(direction: str, entry_price: float, pips_to_use: float) -> float:
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

    if volume_step <= 0: volume_step = 0.01
    lot = round(lot / volume_step) * volume_step
    lot = max(volume_min, lot)
    lot = min(volume_max, lot)
    lot = min(LOT_MAX, lot)

    precision = abs(int(math.log10(volume_step))) if 0 < volume_step < 1 else 0
    return round(lot, precision)

def format_mt5_comment(grid_name: str, action: str) -> str:
    sane_action = "".join(filter(str.isalnum, action))
    if not sane_action: sane_action = "trade"
    comment = f"Grid{grid_name}_{sane_action}"
    return comment[:31]

# --- Trade execution (Market Orders) ---
def send_market_order(direction: str, lot: float, magic: int, grid_name: str, action_comment_str: str, stop_loss: float = 0.0, take_profit: float | None = None) -> mt5.OrderSendResult | None:
    mt5_comment = format_mt5_comment(grid_name, action_comment_str)

    adjusted_lot = adjust_lot(lot)
    symbol_info_vol_min = getattr(mt5.symbol_info(SYMBOL), "volume_min", 0.01)
    if adjusted_lot < symbol_info_vol_min :
        log.warning(f"Market Order: Adjusted lot {adjusted_lot} is below minimum {symbol_info_vol_min} for {direction} {lot} ({mt5_comment}). Order not sent.")
        return None

    price, calculated_tp = 0.0, 0.0
    for attempt in range(5):
        tick = mt5.symbol_info_tick(SYMBOL)
        if tick and hasattr(tick, 'bid') and hasattr(tick, 'ask') and tick.bid > 0 and tick.ask > 0:
            price = tick.ask if direction == "BUY" else tick.bid
            if take_profit is not None:
                calculated_tp = take_profit
            else:
                positions = [p for p in (mt5.positions_get(symbol=SYMBOL) or []) if p.magic == magic]
                is_near_dynamic_trigger = (len(positions) + 1) >= DYNAMIC_POSITIONS_TRIGGER
                pips_to_use = DYNAMIC_PROFIT_PIPS if is_near_dynamic_trigger else PROFIT_PIPS
                calculated_tp = tp_price(direction, price, pips_to_use)
            break
        log.warning(f"Market Order: Attempt {attempt+1}/5: No valid tick for {direction} ({mt5_comment}). Retrying...")
        time.sleep(0.2)
    else:
        log.error(f"Market Order: Failed to get valid tick for {direction} ({mt5_comment}). Cannot send.")
        return None

    if price == 0.0:
        log.error(f"Market Order: Price for {SYMBOL} is zero. Cannot send {direction} ({mt5_comment}).")
        return None

    request = {
        "action": mt5.TRADE_ACTION_DEAL, "symbol": SYMBOL, "volume": adjusted_lot,
        "type": mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL,
        "price": price, "tp": calculated_tp, "sl": stop_loss, "deviation": SLIPPAGE,
        "magic": magic, "comment": mt5_comment, "type_time": mt5.ORDER_TIME_GTC,
    }

    s_info = mt5.symbol_info(SYMBOL)
    if s_info and hasattr(s_info, 'filling_modes'):
        allowed_filling_types = s_info.filling_modes
        if mt5.ORDER_FILLING_IOC in allowed_filling_types: request["type_filling"] = mt5.ORDER_FILLING_IOC
        elif mt5.ORDER_FILLING_FOK in allowed_filling_types: request["type_filling"] = mt5.ORDER_FILLING_FOK
        elif len(allowed_filling_types) > 0: request["type_filling"] = allowed_filling_types[0]

    log.info(f"Sending Market Order: {direction} {adjusted_lot} {SYMBOL} @ Market (Ref: {price:.5f}) TP: {calculated_tp:.5f} SL: {stop_loss:.5f} Magic: {magic} Comment: {mt5_comment}")
    result = mt5.order_send(request)

    if result is None:
        log.error(f"Market Order send failed for {direction} {adjusted_lot} ({mt5_comment}). MT5 returned None. Error: {mt5.last_error()}")
        return None

    if result.retcode == mt5.TRADE_RETCODE_DONE:
        log.info(f"🟢 Market Order: {direction} {result.volume:.2f} @ {result.price:.5f} TP {calculated_tp:.5f} (Magic: {magic}, Ticket: {result.order}, Comment: {mt5_comment}) successfully placed.")
        return result
    else:
        log.error(f"🔴 Market Order send failed for {direction} {adjusted_lot} ({mt5_comment}). Retcode: {result.retcode}, MT5 Comment: {result.comment}, Error: {mt5.last_error()}")
        if result.retcode == 10026:
            log.critical("CRITICAL: Autotrading is disabled on the server. Please enable it in MT5 Terminal (Tools -> Options -> Expert Advisors -> Allow automated trading).")
        return None

# --- Trade execution (Limit Orders) ---
def place_limit_order(direction: str, price_level: float, lot: float, magic: int, grid_name: str, action_comment_str: str) -> mt5.OrderSendResult | None:
    mt5_comment = format_mt5_comment(grid_name, action_comment_str)

    adjusted_lot = adjust_lot(lot)
    symbol_info_vol_min = getattr(mt5.symbol_info(SYMBOL), "volume_min", 0.01)
    if adjusted_lot < symbol_info_vol_min:
        log.warning(f"Limit Order: Adjusted lot {adjusted_lot} is below minimum {symbol_info_vol_min} for {direction} at {price_level:.5f}. Order not placed.")
        return None

    positions = [p for p in (mt5.positions_get(symbol=SYMBOL) or []) if p.magic == magic]
    is_near_dynamic_trigger = (len(positions) + 1) >= DYNAMIC_POSITIONS_TRIGGER
    pips_to_use = DYNAMIC_PROFIT_PIPS if is_near_dynamic_trigger else PROFIT_PIPS
    limit_order_tp = tp_price(direction, price_level, pips_to_use)
    order_type = mt5.ORDER_TYPE_BUY_LIMIT if direction == "BUY" else mt5.ORDER_TYPE_SELL_LIMIT

    request = {
        "action": mt5.TRADE_ACTION_PENDING, "symbol": SYMBOL, "volume": adjusted_lot,
        "type": order_type, "price": price_level, "tp": limit_order_tp, "sl": 0.0,
        "magic": magic, "comment": mt5_comment, "type_time": mt5.ORDER_TIME_GTC,
    }

    if PENDING_ORDER_EXPIRATION_MIN > 0:
        request["type_time"] = mt5.ORDER_TIME_SPECIFIED
        request["expiration"] = int(time.time()) + PENDING_ORDER_EXPIRATION_MIN * 60

    s_info = mt5.symbol_info(SYMBOL)
    if s_info and hasattr(s_info, 'filling_modes'):
        allowed_filling_types = s_info.filling_modes
        if mt5.ORDER_FILLING_RETURN in allowed_filling_types: request["type_filling"] = mt5.ORDER_FILLING_RETURN
        elif len(allowed_filling_types) > 0: request["type_filling"] = allowed_filling_types[0]

    log.info(f"Placing Limit Order: {direction} {adjusted_lot} {SYMBOL} @ {price_level:.5f} TP: {limit_order_tp:.5f} Magic: {magic} Comment: {mt5_comment}")
    result = mt5.order_send(request)

    if result is None:
        log.error(f"Limit Order placement failed (MT5 returned None) for {direction} {adjusted_lot} @ {price_level:.5f} ({mt5_comment}). Error: {mt5.last_error()}")
        return None

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        tick = mt5.symbol_info_tick(SYMBOL)
        log.error(f"🔴 Limit Order placement FAILED for {direction} {adjusted_lot} @ {price_level:.5f} ({mt5_comment}). Retcode: {result.retcode}, MT5 Comment: {result.comment}, Error: {mt5.last_error()}. Current Bid: {tick.bid if tick else 'N/A'}, Ask: {tick.ask if tick else 'N/A'}")
    else:
        log.info(f"🟢 Limit Order: {direction} {result.volume:.2f} {SYMBOL} @ {result.price:.5f} TP {limit_order_tp:.5f} (Magic: {magic}, Order Ticket: {result.order}, Comment: {mt5_comment}) successfully placed.")
    return result

def cancel_pending_orders_by_side(direction: str, magic: int | None = None):
    log.info(f"Attempting to cancel all pending {direction} limit orders for magic {magic if magic is not None else 'ALL'}.")
    order_type_to_cancel = mt5.ORDER_TYPE_BUY_LIMIT if direction == "BUY" else mt5.ORDER_TYPE_SELL_LIMIT

    pending_orders = mt5.orders_get(symbol=SYMBOL) or []
    cancelled_count = 0
    for order in pending_orders:
        if (magic is None or order.magic == magic) and order.type == order_type_to_cancel:
            del_request = {"action": mt5.TRADE_ACTION_REMOVE, "order": order.ticket, "symbol": SYMBOL}
            del_res = mt5.order_send(del_request)
            if del_res and del_res.retcode == mt5.TRADE_RETCODE_DONE:
                log.info(f"Cancelled pending {direction} limit order {order.ticket} for magic {order.magic}")
                cancelled_count += 1
            else:
                log.error(f"Failed to cancel pending {direction} limit order {order.ticket} (magic {order.magic}). Error: {mt5.last_error()}, Retcode: {del_res.retcode if del_res else 'N/A'}")
    if cancelled_count > 0:
        log.info(f"Cancelled {cancelled_count} pending {direction} limit orders.")
    else:
        log.info(f"No pending {direction} limit orders found for magic {magic if magic is not None else 'ALL'} to cancel.")


def sync_all_tps(direction: str, magic: int):
    log.debug(f"Attempting to sync TPs for {direction} side for magic {magic}.")

    grid_state = grid_states.get(magic)
    if not grid_state:
        log.warning(f"sync_all_tps: Could not find state for magic {magic}. Skipping sync.")
        return

    if direction == "BUY" and grid_state.get('capped_buy', False):
        log.debug(f"TP Sync for BUY side (magic {magic}) skipped because this side is marked as capped in the grid state.")
        return
    if direction == "SELL" and grid_state.get('capped_sell', False):
        log.debug(f"TP Sync for SELL side (magic {magic}) skipped because this side is marked as capped in the grid state.")
        return

    side_to_sync = mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL
    all_positions_of_side = [
        p for p in (mt5.positions_get(symbol=SYMBOL) or [])
        if p.magic == magic and p.type == side_to_sync
        and SEARCH_KEYWORD_CAPPED_GENERIC not in p.comment
        and SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC not in p.comment
    ]

    if not all_positions_of_side:
        log.debug(f"No open, non-capped, non-dynamic-hedge {direction} positions for magic {magic} to sync TPs for.")
        return

    try:
        all_positions_of_side.sort(key=lambda p: (p.time_msc if hasattr(p, 'time_msc') and p.time_msc > 0 else p.time, p.ticket), reverse=True)
        newest_position = all_positions_of_side[0]
    except IndexError:
        log.error(f"Could not determine newest position for {direction} side (magic {magic}) for TP sync (list was empty).")
        return

    if newest_position.tp == 0:
        log.debug(f"Newest eligible {direction} position {newest_position.ticket} (magic {magic}) has no TP. Nothing to sync.")
        return

    new_tp_level = newest_position.tp
    log.debug(f"TP Sync check for {direction} (magic {magic}): Newest eligible position {newest_position.ticket} TP is {new_tp_level:.5f}. Comparing with others.")
    synced_count = 0
    mt5_comment_sync = format_mt5_comment(grid_states[magic]['name'], f"sync_tp_{newest_position.ticket}")

    for p in all_positions_of_side:
        if p.ticket == newest_position.ticket or math.isclose(p.tp, new_tp_level, abs_tol=mt5.symbol_info(SYMBOL).point * 0.1):
            continue

        log.info(f"TP Sync for {direction} (magic {magic}): Position {p.ticket} (TP: {p.tp:.5f}) will be synced to {new_tp_level:.5f}")
        modify_request = {
            "action": mt5.TRADE_ACTION_SLTP, "symbol": SYMBOL, "position": p.ticket,
            "tp": new_tp_level, "sl": p.sl, "magic": magic, "comment": mt5_comment_sync
        }
        result = mt5.order_send(modify_request)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(f"🔄 TP synced for {direction} position {p.ticket} to {new_tp_level:.5f}")
            synced_count += 1
        else:
            log.warning(f"sync_all_tps: Failed to sync TP for {direction} position {p.ticket}. Error: {mt5.last_error()}, Retcode: {result.retcode if result else 'N/A'}")

    if synced_count > 0:
        log.info(f"✅ TP Sync Summary for {direction} (magic {magic}): {synced_count} positions updated to TP {new_tp_level:.5f}")
    else:
        log.debug(f"No TPs needed syncing for {direction} side (magic {magic}) based on newest eligible position {newest_position.ticket}.")

# --- Utility to close all positions for the symbol ---
def close_all_symbol_positions(reason_comment_suffix: str):
    """Closes all open positions for the configured SYMBOL."""
    log.info(f"Attempting to close all positions for {SYMBOL} due to: {reason_comment_suffix}")
    open_positions = mt5.positions_get(symbol=SYMBOL) or []
    closed_count = 0
    if not open_positions:
        log.info(f"No open positions found for {SYMBOL} to close for {reason_comment_suffix}.")
        return

    for position in open_positions:
        close_direction = mt5.ORDER_TYPE_SELL if position.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        tick = mt5.symbol_info_tick(SYMBOL)
        if not tick or tick.bid == 0 or tick.ask == 0:
            log.error(f"Could not get valid tick to close position {position.ticket} for {reason_comment_suffix}. Skipping.")
            continue
        close_price = tick.bid if position.type == mt5.ORDER_TYPE_BUY else tick.ask

        close_request = {
            "action": mt5.TRADE_ACTION_DEAL, "symbol": SYMBOL, "volume": position.volume,
            "type": close_direction, "position": position.ticket, "price": close_price,
            "deviation": SLIPPAGE + 20, "magic": position.magic,
            "comment": format_mt5_comment(SYSTEM_COMMENT_GRID_NAME, reason_comment_suffix)
        }
        s_info = mt5.symbol_info(SYMBOL)
        if s_info and hasattr(s_info, 'filling_modes'):
            allowed = s_info.filling_modes
            if mt5.ORDER_FILLING_IOC in allowed: close_request["type_filling"] = mt5.ORDER_FILLING_IOC
            elif mt5.ORDER_FILLING_FOK in allowed: close_request["type_filling"] = mt5.ORDER_FILLING_FOK
            elif len(allowed) > 0: close_request["type_filling"] = allowed[0]

        result = mt5.order_send(close_request)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(f"Closed position {position.ticket} (Vol: {position.volume}, Type: {'BUY' if position.type == mt5.ORDER_TYPE_BUY else 'SELL'}) for {reason_comment_suffix}.")
            closed_count +=1
        else:
            log.error(f"Failed to close position {position.ticket} for {reason_comment_suffix}. Error: {mt5.last_error()}, Retcode: {result.retcode if result else 'N/A'}")
            if result and result.retcode == 10026: log.critical("CRITICAL: Autotrading is disabled on the server.")
    log.info(f"Closed {closed_count}/{len(open_positions)} open positions for {SYMBOL} due to {reason_comment_suffix}.")

# --- Auto-hedge handling (using Market Orders) ---
def hedge_if_empty():
    global grid_states
    if grid_states or (mt5.positions_get(symbol=SYMBOL) or []):
        return

    if ENABLE_TRADING_HOURS and not is_trading_session_active():
        if loop_counter % (LOG_BALANCE_INTERVAL * 10) == 1 :
            log.info(f"hedge_if_empty: Outside trading hours ({TRADING_START_TIME_STR} - {TRADING_END_TIME_STR} UTC). Initial hedge not placed.")
        return

    log.info("🔄 No active grids and no positions found. Initiating initial grid (Magic: %d)...", base_magic_number)
    buy_result = send_market_order("BUY", LOT_SMALL, base_magic_number, "A", ACTION_INITIAL_HEDGE_BUY)
    time.sleep(0.2) # Small delay to allow order processing
    sell_result = send_market_order("SELL", LOT_SMALL, base_magic_number, "A", ACTION_INITIAL_HEDGE_SELL)

    if buy_result and sell_result:
        # The result object for a market order contains the deal price.
        buy_anchor_price = buy_result.price
        sell_anchor_price = sell_result.price

        grid_states[base_magic_number] = {
            "name": "A",
            "buy_anchor_price": buy_anchor_price,
            "sell_anchor_price": sell_anchor_price,
            "buy_sequence_index": 0, # Next grid trade to place is index 0 from sequence
            "sell_sequence_index": 0,
            "prev_buy_count": 1, "prev_sell_count": 1,
            "capped_buy": False, "capped_sell": False
        }
        log.info(f"Initial market hedge placed for Grid A (magic {base_magic_number}). BUY Anchor: {buy_anchor_price:.5f}, SELL Anchor: {sell_anchor_price:.5f}")
        time.sleep(OPEN_DELAY)
    else:
        log.error("Failed to place one or both initial market hedge orders. Will retry on next loop.")


# --- Closed hedge detection (using Market Orders) ---
def handle_closed_hedge(magic: int):
    global grid_states
    grid_state = grid_states.get(magic)
    if not grid_state: return

    positions_in_grid = [p for p in (mt5.positions_get(symbol=SYMBOL) or []) if p.magic == magic]
    current_buy_count = sum(1 for p in positions_in_grid if p.type == mt5.ORDER_TYPE_BUY and SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC not in p.comment)
    current_sell_count = sum(1 for p in positions_in_grid if p.type == mt5.ORDER_TYPE_SELL and SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC not in p.comment)

    # Check if a BUY position closed
    if current_buy_count < grid_state.get("prev_buy_count", current_buy_count + 1):
        log.info(f"🔔 BUY position closed in grid {grid_state['name']} (magic {magic}). Current non-hedge BUY count: {current_buy_count}.")
        if not grid_state.get('capped_buy'):
            if current_buy_count == 0 and current_sell_count > 0:
                log.info(f"Grid {grid_state['name']} BUY side is now empty. Resetting and re-hedging.")
                cancel_pending_orders_by_side("BUY", magic) # Cancel any stragglers
                if not (ENABLE_TRADING_HOURS and not is_trading_session_active()):
                    log.info(f"Grid {grid_state['name']} BUY side empty, SELL side active. Re-hedging BUY.")
                    buy_rehedge_result = send_market_order("BUY", LOT_SMALL, magic, grid_state['name'], action_comment_str=ACTION_RE_HEDGE_BUY)
                    if buy_rehedge_result:
                        grid_state["buy_anchor_price"] = buy_rehedge_result.price
                        grid_state["buy_sequence_index"] = 0
                        log.info(f"Grid {grid_state['name']} BUY side re-hedged. New anchor price: {buy_rehedge_result.price:.5f}")
                        time.sleep(OPEN_DELAY)
                else:
                    log.info(f"Grid {grid_state['name']} BUY side re-hedge skipped: Outside trading hours.")
            # If not fully closed, we do nothing. step_grid will handle placing new orders.

    # Check if a SELL position closed
    if current_sell_count < grid_state.get("prev_sell_count", current_sell_count + 1):
        log.info(f"🔔 SELL position closed in grid {grid_state['name']} (magic {magic}). Current non-hedge SELL count: {current_sell_count}.")
        if not grid_state.get('capped_sell'):
            if current_sell_count == 0 and current_buy_count > 0:
                log.info(f"Grid {grid_state['name']} SELL side is now empty. Resetting and re-hedging.")
                cancel_pending_orders_by_side("SELL", magic)
                if not (ENABLE_TRADING_HOURS and not is_trading_session_active()):
                    log.info(f"Grid {grid_state['name']} SELL side empty, BUY side active. Re-hedging SELL.")
                    sell_rehedge_result = send_market_order("SELL", LOT_SMALL, magic, grid_state['name'], action_comment_str=ACTION_RE_HEDGE_SELL)
                    if sell_rehedge_result:
                        grid_state["sell_anchor_price"] = sell_rehedge_result.price
                        grid_state["sell_sequence_index"] = 0
                        log.info(f"Grid {grid_state['name']} SELL side re-hedged. New anchor price: {sell_rehedge_result.price:.5f}")
                        time.sleep(OPEN_DELAY)
                else:
                    log.info(f"Grid {grid_state['name']} SELL side re-hedge skipped: Outside trading hours.")

    grid_state["prev_buy_count"] = current_buy_count
    grid_state["prev_sell_count"] = current_sell_count

    if not positions_in_grid:
        log.warning(f"Grid {grid_state['name']} (magic {magic}) is now completely empty. Deactivating this grid.")
        cancel_pending_orders_by_side("BUY", magic)
        cancel_pending_orders_by_side("SELL", magic)
        if magic in grid_states:
            del grid_states[magic]
            log.info(f"Grid state for magic {magic} removed.")


# --- Grid stepping logic (using Limit Orders) ---
def step_grid(magic: int):
    global grid_states
    grid_state = grid_states.get(magic)
    if not grid_state: return

    if len(mt5.positions_get(symbol=SYMBOL) or []) >= MAX_POSITIONS:
        log.warning(f"Max open positions ({MAX_POSITIONS}) reached. No new grid limit orders will be placed for magic {magic}.")
        return

    if ENABLE_TRADING_HOURS and not is_trading_session_active():
        if loop_counter % (LOG_BALANCE_INTERVAL * 10) == 1 :
            log.info(f"step_grid (Magic {magic}): Outside trading hours. No new limit orders will be placed.")
        return

    s_info = mt5.symbol_info(SYMBOL)
    if not s_info: return # Should not happen if login is successful
    s_info_digits = s_info.digits

    pending_orders = [o for o in (mt5.orders_get(symbol=SYMBOL) or []) if o.magic == magic]
    current_pending_buy_limits = sum(1 for o in pending_orders if o.type == mt5.ORDER_TYPE_BUY_LIMIT)
    current_pending_sell_limits = sum(1 for o in pending_orders if o.type == mt5.ORDER_TYPE_SELL_LIMIT)

    # --- BUY LIMIT ---
    if not grid_state.get('capped_buy', False) and 'buy_anchor_price' in grid_state and grid_state['buy_anchor_price'] is not None:
        while current_pending_buy_limits < MAX_PENDING_GRID_ORDERS_PER_SIDE:
            buy_seq_idx = grid_state.get("buy_sequence_index", 0)
            if buy_seq_idx >= len(GRID_LOT_SEQUENCE):
                log.warning(f"BUY side for magic {magic} has reached the end of the lot sequence ({len(GRID_LOT_SEQUENCE)}). No more BUY orders will be placed.")
                break

            raw_lot = GRID_LOT_SEQUENCE[buy_seq_idx]
            current_lot = adjust_lot(raw_lot)

            # Determine step pips based on position count
            positions = [p for p in (mt5.positions_get(symbol=SYMBOL) or []) if p.magic == magic]
            num_buys = sum(1 for p in positions if p.type == mt5.ORDER_TYPE_BUY)
            step_pips = DYNAMIC_STEP_PIPS if num_buys >= DYNAMIC_POSITIONS_TRIGGER else GRID_STEP_PIPS

            # Calculate price based on anchor and sequence index
            # The (buy_seq_idx + 1) ensures the first grid order (index 0) is 1 step away
            price_offset = (buy_seq_idx + 1) * step_pips * pip_val()
            current_price_level = round(grid_state['buy_anchor_price'] - price_offset, s_info_digits)

            log.info(f"Attempting to place BUY_LIMIT for magic {magic} at level {buy_seq_idx + 1}, price {current_price_level:.5f}")
            order_result = place_limit_order("BUY", current_price_level, current_lot, magic, grid_state['name'], ACTION_GRID_LIMIT_BUY)

            if order_result and order_result.retcode == mt5.TRADE_RETCODE_DONE:
                grid_state["buy_sequence_index"] += 1
                current_pending_buy_limits += 1
                log.info(f"Successfully placed BUY_LIMIT for magic {magic}. New sequence index: {grid_state['buy_sequence_index']}.")
                time.sleep(OPEN_DELAY) # Small delay between placing orders
            elif order_result and order_result.retcode == 10015: # Invalid price
                log.warning(f"BUY_LIMIT failed due to Invalid Price for magic {magic} at level {current_price_level:.5f}. This can happen if price moved significantly. The grid will attempt to place the next level on the next cycle.")
                break # Break the while loop to avoid spamming invalid orders
            else:
                log.error(f"Failed to place BUY_LIMIT for magic {magic}. Breaking loop for this cycle.")
                break # Break on other errors

    # --- SELL LIMIT ---
    if not grid_state.get('capped_sell', False) and 'sell_anchor_price' in grid_state and grid_state['sell_anchor_price'] is not None:
        while current_pending_sell_limits < MAX_PENDING_GRID_ORDERS_PER_SIDE:
            sell_seq_idx = grid_state.get("sell_sequence_index", 0)
            if sell_seq_idx >= len(GRID_LOT_SEQUENCE):
                log.warning(f"SELL side for magic {magic} has reached the end of the lot sequence ({len(GRID_LOT_SEQUENCE)}). No more SELL orders will be placed.")
                break

            raw_lot = GRID_LOT_SEQUENCE[sell_seq_idx]
            current_lot = adjust_lot(raw_lot)

            positions = [p for p in (mt5.positions_get(symbol=SYMBOL) or []) if p.magic == magic]
            num_sells = sum(1 for p in positions if p.type == mt5.ORDER_TYPE_SELL)
            step_pips = DYNAMIC_STEP_PIPS if num_sells >= DYNAMIC_POSITIONS_TRIGGER else GRID_STEP_PIPS

            price_offset = (sell_seq_idx + 1) * step_pips * pip_val()
            current_price_level = round(grid_state['sell_anchor_price'] + price_offset, s_info_digits)

            log.info(f"Attempting to place SELL_LIMIT for magic {magic} at level {sell_seq_idx + 1}, price {current_price_level:.5f}")
            order_result = place_limit_order("SELL", current_price_level, current_lot, magic, grid_state['name'], ACTION_GRID_LIMIT_SELL)

            if order_result and order_result.retcode == mt5.TRADE_RETCODE_DONE:
                grid_state["sell_sequence_index"] += 1
                current_pending_sell_limits += 1
                log.info(f"Successfully placed SELL_LIMIT for magic {magic}. New sequence index: {grid_state['sell_sequence_index']}.")
                time.sleep(OPEN_DELAY)
            elif order_result and order_result.retcode == 10015:
                log.warning(f"SELL_LIMIT failed due to Invalid Price for magic {magic} at level {current_price_level:.5f}. The grid will attempt to place the next level on the next cycle.")
                break
            else:
                log.error(f"Failed to place SELL_LIMIT for magic {magic}. Breaking loop for this cycle.")
                break


# --- Dynamic Trigger and Internal Capping/Hedging Logic ---
def handle_grid_trigger_and_cap():
    global grid_states, next_magic_number

    for magic in list(grid_states.keys()):
        grid_state = grid_states.get(magic)
        if not grid_state: continue

        positions = [p for p in (mt5.positions_get(symbol=SYMBOL) or []) if p.magic == magic]
        buy_positions = [p for p in positions if p.type == mt5.ORDER_TYPE_BUY]
        sell_positions = [p for p in positions if p.type == mt5.ORDER_TYPE_SELL]

        log.debug(f"Checking dynamic trigger for magic {magic} ({grid_state['name']}): Buys={len(buy_positions)}, Sells={len(sell_positions)}, TriggerLimit={DYNAMIC_POSITIONS_TRIGGER}")

        # --- Process BUY side trigger ---
        if not grid_state.get("capped_buy", False) and len(buy_positions) >= DYNAMIC_POSITIONS_TRIGGER:
            log.warning(f"🚨 DYNAMIC TRIGGER HIT for BUY side on grid {grid_state['name']} (magic {magic}). Freezing grid and hedging internally.")
            positions_to_cap = [p for p in buy_positions if SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC not in p.comment]
            if not positions_to_cap:
                log.warning(f"BUY side trigger for {grid_state['name']}, but no non-hedge positions found to cap. Skipping.")
                continue

            total_volume_to_cap = sum(p.volume for p in positions_to_cap)
            hedge_lot = adjust_lot(total_volume_to_cap / 2.0)
            positions_to_cap.sort(key=lambda p: (p.time_msc if hasattr(p, 'time_msc') and p.time_msc > 0 else p.time, p.ticket), reverse=True)
            last_pos_to_cap = positions_to_cap[0]
            sl_for_hedge = last_pos_to_cap.tp if last_pos_to_cap.tp > 0 else 0.0

            log.info(f"Grid {grid_state['name']} BUY side triggered. Attempting internal SELL hedge: Lot {hedge_lot:.2f}, SL {sl_for_hedge:.5f}")
            if send_market_order("SELL", hedge_lot, magic, grid_state['name'], ACTION_DYNAMIC_HEDGE_SELL_TAG, stop_loss=sl_for_hedge, take_profit=0.0) is not None:
                log.info(f"✅ Internal SELL hedge placed for grid {grid_state['name']}. Now freezing grid and starting new one.")
                freeze_grid_and_start_new(magic, grid_state, positions_to_cap, ACTION_CAPPED_BUY_TAG)
            else:
                log.error(f"Failed to place internal SELL hedge for grid {grid_state['name']}. Grid not frozen. Trigger remains active.")

        # --- Process SELL side trigger ---
        elif not grid_state.get("capped_sell", False) and len(sell_positions) >= DYNAMIC_POSITIONS_TRIGGER:
            log.warning(f"🚨 DYNAMIC TRIGGER HIT for SELL side on grid {grid_state['name']} (magic {magic}). Freezing grid and hedging internally.")
            positions_to_cap = [p for p in sell_positions if SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC not in p.comment]
            if not positions_to_cap:
                log.warning(f"SELL side trigger for {grid_state['name']}, but no non-hedge positions found to cap. Skipping.")
                continue

            total_volume_to_cap = sum(p.volume for p in positions_to_cap)
            hedge_lot = adjust_lot(total_volume_to_cap / 2.0)
            positions_to_cap.sort(key=lambda p: (p.time_msc if hasattr(p, 'time_msc') and p.time_msc > 0 else p.time, p.ticket), reverse=True)
            last_pos_to_cap = positions_to_cap[0]
            sl_for_hedge = last_pos_to_cap.tp if last_pos_to_cap.tp > 0 else 0.0

            log.info(f"Grid {grid_state['name']} SELL side triggered. Attempting internal BUY hedge: Lot {hedge_lot:.2f}, SL {sl_for_hedge:.5f}")
            if send_market_order("BUY", hedge_lot, magic, grid_state['name'], ACTION_DYNAMIC_HEDGE_BUY_TAG, stop_loss=sl_for_hedge, take_profit=0.0) is not None:
                log.info(f"✅ Internal BUY hedge placed for grid {grid_state['name']}. Now freezing grid and starting new one.")
                freeze_grid_and_start_new(magic, grid_state, positions_to_cap, ACTION_CAPPED_SELL_TAG)
            else:
                log.error(f"Failed to place internal BUY hedge for grid {grid_state['name']}. Grid not frozen. Trigger remains active.")

def freeze_grid_and_start_new(magic: int, grid_state: dict, positions_to_cap: list, cap_action_tag: str):
    """Helper function to perform the grid freezing and new grid creation logic."""
    global next_magic_number

    log.info(f"Updating comments for {len(positions_to_cap)} positions in grid {grid_state['name']} (magic {magic}).")
    for p in positions_to_cap:
        capped_comment_str = format_mt5_comment(grid_state['name'], cap_action_tag)
        modify_request = {
            "action": mt5.TRADE_ACTION_SLTP, "position": p.ticket,
            "tp": p.tp, "sl": p.sl, "magic": magic, "comment": capped_comment_str
        }
        result = mt5.order_send(modify_request)
        if not (result and result.retcode == mt5.TRADE_RETCODE_DONE):
            log.warning(f"Failed to modify/cap position {p.ticket}. Retcode: {result.retcode if result else 'N/A'}, Error: {mt5.last_error()}")

    log.warning(f"Grid {grid_state['name']} (magic {magic}) is now fully frozen. No new positions will be opened on EITHER side of this grid.")
    cancel_pending_orders_by_side("BUY", magic)
    cancel_pending_orders_by_side("SELL", magic)
    grid_state.update({'capped_buy': True, 'capped_sell': True})

    if len(grid_states) < MAX_ACTIVE_GRIDS:
        if ENABLE_TRADING_HOURS and not is_trading_session_active():
            log.info(f"Grid {grid_state['name']} frozen. Outside trading hours. Not creating new independent grid.")
            return

        log.info(f"Grid {grid_state['name']} frozen. Attempting to create new independent grid.")
        new_magic = next_magic_number
        new_grid_name = chr(ord(grid_state['name']) + 1)
        log.info(f"New independent grid will be Grid {new_grid_name} with magic {new_magic}.")
        
        buy_result = send_market_order("BUY", LOT_SMALL, new_magic, new_grid_name, ACTION_INITIAL_HEDGE_BUY)
        time.sleep(0.2)
        sell_result = send_market_order("SELL", LOT_SMALL, new_magic, new_grid_name, ACTION_INITIAL_HEDGE_SELL)

        if buy_result and sell_result:
            buy_anchor_price = buy_result.price
            sell_anchor_price = sell_result.price
            grid_states[new_magic] = {
                "name": new_grid_name,
                "buy_anchor_price": buy_anchor_price,
                "sell_anchor_price": sell_anchor_price,
                "buy_sequence_index": 0,
                "sell_sequence_index": 0,
                "prev_buy_count": 1, "prev_sell_count": 1,
                "capped_buy": False, "capped_sell": False
            }
            log.info(f"✅ Successfully created new independent grid {new_grid_name} (magic {new_magic}). BUY Anchor: {buy_anchor_price:.5f}, SELL Anchor: {sell_anchor_price:.5f}")
            next_magic_number += 1
            time.sleep(OPEN_DELAY)
        else:
            log.error(f"Failed to place initial hedge orders for new grid {new_grid_name} (magic {new_magic}). New grid not activated.")
    else:
        log.info(f"Grid {grid_state['name']} frozen. Max active grids ({MAX_ACTIVE_GRIDS}) reached. Not creating new grid.")


# --- Startup and Main Loop ---
def log_initial_parameters():
    """Logs the bot's starting parameters."""
    log.info(f"Starting Grid Martingale Bot (v110.11-StaticGrid-Refined)")
    log.info(f"SYMBOL: {SYMBOL}, LOT_SMALL: {LOT_SMALL}, MAX_LOT: {LOT_MAX}")
    log.info(f"GRID_PIPS: {GRID_STEP_PIPS}, PROFIT_PIPS: {PROFIT_PIPS}, DYN_TRIGGER: {DYNAMIC_POSITIONS_TRIGGER}")
    log.info(f"MAX_POS: {MAX_POSITIONS}, MAX_ACTIVE_GRIDS: {MAX_ACTIVE_GRIDS}, PROFIT_TARGET: {PROFIT_TARGET_AMT}")
    log.info(f"MAX_LOSS_AMT: {MAX_LOSS_AMT if MAX_LOSS_AMT > 0 else 'Disabled'}")
    log.info(f"SLIPPAGE: {SLIPPAGE}, INVALID_PRICE_RETRY_LIMIT: {INVALID_PRICE_RETRY_LIMIT}")
    if ENABLE_TRADING_HOURS:
        if TRADING_START_TIME_OBJ and TRADING_END_TIME_OBJ:
            log.info(f"TRADING HOURS ENABLED: {TRADING_START_TIME_OBJ.strftime('%H:%M')} - {TRADING_END_TIME_OBJ.strftime('%H:%M')} UTC")
        else:
            log.warning(f"Trading hours were enabled but failed parsing. TRADING_START_TIME_STR: '{TRADING_START_TIME_STR}', TRADING_END_TIME_STR: '{TRADING_END_TIME_STR}'.")
            log.info(f"TRADING HOURS DISABLED due to parsing error.")
    else:
        log.info(f"TRADING HOURS DISABLED.")

def reconstruct_state_on_restart():
    """Reconstructs the bot's state from existing positions and orders on startup."""
    global grid_states, next_magic_number

    existing_positions = mt5.positions_get(symbol=SYMBOL) or []
    pending_orders = mt5.orders_get(symbol=SYMBOL) or []
    if existing_positions:
        magics = sorted(list(set(p.magic for p in existing_positions)))
        log.info(f"↻ Restarted. Found existing positions with magic numbers: {magics}")

        if magics:
            current_max_magic = max(magics)
            if current_max_magic >= base_magic_number:
                 next_magic_number = current_max_magic + 1

        for i, magic in enumerate(magics):
            grid_name_from_comment = None
            for p in existing_positions:
                if p.magic == magic and p.comment and p.comment.startswith("Grid") and "_" in p.comment:
                    try:
                        potential_name = p.comment.split("Grid")[1].split("_")[0]
                        if len(potential_name) == 1 and 'A' <= potential_name <= 'Z':
                            grid_name_from_comment = potential_name
                            break
                    except IndexError: pass
            grid_name = grid_name_from_comment if grid_name_from_comment else chr(ord('A') + i % 26)

            positions_in_grid = [p for p in existing_positions if p.magic == magic]
            buy_positions = [p for p in positions_in_grid if p.type == mt5.ORDER_TYPE_BUY]
            sell_positions = [p for p in positions_in_grid if p.type == mt5.ORDER_TYPE_SELL]

            # --- Reconstruct BUY side state ---
            buy_anchor_price = None
            buy_sequence_index = 0
            if buy_positions:
                # Anchor is the earliest non-dynamic-hedge position
                eligible_buy_anchors = sorted([p for p in buy_positions if SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC not in p.comment], key=lambda p: p.time)
                if eligible_buy_anchors:
                    buy_anchor_price = eligible_buy_anchors[0].price_open

                # Sequence index is the number of grid trades already opened (filled or pending)
                filled_grid_buys = sum(1 for p in buy_positions if (SEARCH_KEYWORD_GRIDBUY_SANE in p.comment or SEARCH_KEYWORD_MARKET_FALLBACK_SUBSTRING in p.comment) and SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC not in p.comment)
                pending_grid_buys = sum(1 for o in pending_orders if o.magic == magic and o.type == mt5.ORDER_TYPE_BUY_LIMIT)
                buy_sequence_index = filled_grid_buys + pending_grid_buys

            # --- Reconstruct SELL side state ---
            sell_anchor_price = None
            sell_sequence_index = 0
            if sell_positions:
                eligible_sell_anchors = sorted([p for p in sell_positions if SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC not in p.comment], key=lambda p: p.time)
                if eligible_sell_anchors:
                    sell_anchor_price = eligible_sell_anchors[0].price_open

                filled_grid_sells = sum(1 for p in sell_positions if (SEARCH_KEYWORD_GRIDSELL_SANE in p.comment or SEARCH_KEYWORD_MARKET_FALLBACK_SUBSTRING in p.comment) and SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC not in p.comment)
                pending_grid_sells = sum(1 for o in pending_orders if o.magic == magic and o.type == mt5.ORDER_TYPE_SELL_LIMIT)
                sell_sequence_index = filled_grid_sells + pending_grid_sells

            capped_buy = any(SEARCH_KEYWORD_CAPPED_BUY_SPECIFIC in p.comment for p in buy_positions)
            capped_sell = any(SEARCH_KEYWORD_CAPPED_SELL_SPECIFIC in p.comment for p in sell_positions)

            grid_states[magic] = {
                "name": grid_name,
                "buy_anchor_price": buy_anchor_price, "sell_anchor_price": sell_anchor_price,
                "buy_sequence_index": buy_sequence_index, "sell_sequence_index": sell_sequence_index,
                "prev_buy_count": sum(1 for p in buy_positions if SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC not in p.comment),
                "prev_sell_count": sum(1 for p in sell_positions if SEARCH_KEYWORD_DYNAMIC_HEDGE_GENERIC not in p.comment),
                "capped_buy": capped_buy, "capped_sell": capped_sell,
            }
            log.info(f"Grid {grid_name} (magic {magic}) reconstructed: Buys={len(buy_positions)} (Capped: {capped_buy}), Sells={len(sell_positions)} (Capped: {capped_sell})")
            log.info(f" -> BUY Anchor: {buy_anchor_price or 'N/A'}, Next Idx: {buy_sequence_index}. SELL Anchor: {sell_anchor_price or 'N/A'}, Next Idx: {sell_sequence_index}")
    else:
        hedge_if_empty()

def run():
    global initial_equity, loop_counter

    log_initial_parameters()
    mt5_login()
    reconstruct_state_on_restart()

    try:
        while True:
            loop_counter += 1
            if mt5.terminal_info() is None:
                log.error("Lost connection to MT5 terminal. Attempting to reconnect...")
                mt5.shutdown()
                time.sleep(10)
                mt5_login()
                if mt5.terminal_info() is None:
                    log.critical("Failed to reconnect. Exiting.")
                    break

            if loop_counter % LOG_BALANCE_INTERVAL == 0:
                acc_info = mt5.account_info()
                if acc_info:
                    log.info(f"Account Status - Balance: {acc_info.balance:.2f}, Equity: {acc_info.equity:.2f}")

            # --- Profit/Loss Checks ---
            if PROFIT_TARGET_AMT > 0 or MAX_LOSS_AMT > 0:
                account_info = mt5.account_info()
                if account_info:
                    if PROFIT_TARGET_AMT > 0 and (account_info.equity - initial_equity) >= PROFIT_TARGET_AMT:
                        log.warning(f"🎯 PROFIT TARGET of {PROFIT_TARGET_AMT:.2f} REACHED! Profit: {(account_info.equity - initial_equity):.2f}. Equity: {account_info.equity:.2f}")
                        cancel_pending_orders_by_side("BUY", None)
                        cancel_pending_orders_by_side("SELL", None)
                        close_all_symbol_positions(ACTION_SUFFIX_PROFIT_TARGET_CLOSE)
                        log.info("Resetting bot state for new cycle...")
                        grid_states.clear()
                        new_acc_info = mt5.account_info()
                        initial_equity = new_acc_info.equity if new_acc_info else account_info.equity
                        log.info(f"New initial_equity for profit tracking: {initial_equity:.2f}")
                        hedge_if_empty()
                        continue

                    if MAX_LOSS_AMT > 0 and (initial_equity - account_info.equity) >= MAX_LOSS_AMT:
                        log.critical(f"☠️ MAXIMUM LOSS LIMIT of {MAX_LOSS_AMT:.2f} REACHED! Loss: {(initial_equity - account_info.equity):.2f}. Equity: {account_info.equity:.2f}")
                        cancel_pending_orders_by_side("BUY", None)
                        cancel_pending_orders_by_side("SELL", None)
                        close_all_symbol_positions(ACTION_SUFFIX_MAX_LOSS_CLOSE)
                        log.critical("BOT STOPPING DUE TO MAX LOSS LIMIT REACHED.")
                        sys.exit(1)

            # --- Core Trading Logic ---
            handle_grid_trigger_and_cap()

            for magic_key in list(grid_states.keys()):
                if magic_key not in grid_states: continue
                handle_closed_hedge(magic_key)
                if magic_key not in grid_states: continue
                step_grid(magic_key)
                sync_all_tps("BUY", magic_key)
                sync_all_tps("SELL", magic_key)

            hedge_if_empty()

            time.sleep(LOOP_MS / 1000.0)

    except KeyboardInterrupt:
        log.info("User requested shutdown (KeyboardInterrupt).")
    except Exception as e:
        log.error(f"An unexpected error occurred in the main loop: {e}")
        log.error(traceback.format_exc())
    finally:
        log.info("Shutting down MetaTrader 5 connection...")
        mt5.shutdown()
        log.info("Bot stopped.")

if __name__ == '__main__':
    run()