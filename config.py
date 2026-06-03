# --- Contract addresses -------------
CTF_EXCHANGE_V2           = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_CTF_EXCHANGE_V2  = "0xe2222d279d744050d28e00520010520000310F59"

# --- OrderFilled event topic hash --------------------------------------------
# *Update if the contract ABI ever changes.
ORDER_FILLED_TOPIC        = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"

# --- Parquet writer ----------------------------------------------------------
# Buffer is "flushed" (written to disk) when EITHER threshold is crossed first.
FLUSH_ROWS                = 100   # flush after this many buffered rows
FLUSH_INTERVAL_S          = 10    # flush after this many seconds

# --- Hashmap / unmatched OrderFilled event removal -----------------------------------------------
TTL_SECONDS               = 60    # max number of seconds to keep unmatched OrderFilled events in the hashmap
TTL_CHECK_INTERVAL_S      = 30    # how often the hashmap is checked for stale OrderFilled events 

# --- Reconnection (exponential backoff) --------------------------------------
RECONNECT_BASE_S          = 1     # initial retry delay in seconds
RECONNECT_MAX_S           = 30    # maximum retry delay in seconds

# --- Startup synchronisation -------------------------------------------------
SUB_ACK_TIMEOUT_S         = 30    # max seconds to wait for subscription ack

# --- Output ------------------------------------------------------------------
OUTPUT_DIR                = "output"