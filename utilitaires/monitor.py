import os
import pandas as pd
import discord
from discord.ext import tasks
import pwd
import subprocess
import io
import logging
import asyncio
import functools
import typing
import re  # <— add this import
import datetime

# --- Logging setup for systemd/journald ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def drop_privileges():
    if os.getuid() != 0:
        return  # Déjà exécuté en tant qu'utilisateur normal
    
    # Identifiants de l'utilisateur scraping
    user_name = "scraping"
    user_info = pwd.getpwnam(user_name)
    user_uid = user_info.pw_uid
    user_gid = user_info.pw_gid
    
    # Définir le groupe
    os.setgid(user_gid)
    # Définir l'utilisateur
    os.setuid(user_uid)
    # Définir le HOME
    os.environ['HOME'] = f'/home/{user_name}'

# Changer d'utilisateur
drop_privileges()

# 1) Configure your files, columns, and webhook URL here:
FILES_TO_MONITOR = {
    "/home/scraping/algo_scraping/AMAZON/amazon_offers.csv": ["pfid","idsmartphone","url","timestamp","Price","shipcost","seller","rating","offertype","descriptsmartphone","batch_id"],
    "/home/scraping/algo_scraping/CARREFOUR/scraping_carrefour.csv": ["Platform","Product Name","Seller","Delivery Info","Price","Seller Rating","Timestamp","batch_id"],
    "/home/scraping/algo_scraping/CDISCOUNT/scraping_cdiscount.csv": ["Platform","Product Name","Price","Product state","Seller","Seller Status","Seller Rating","Delivery Fee","Timestamp","Batch ID"],
    "/home/scraping/algo_scraping/LECLERC/product_details.csv": ["Platform","Product Name","Seller","Price","Delivery Fees","Delivery Date","Product State","Seller Rating","Timestamp","batch_id"],
    "/home/scraping/algo_scraping/RAKUTEN/Rakuten_data.csv": ["pfid","idsmartphone","url","timestamp","price","shipcost","rating","ratingnb","offertype","shipcountry","sellercountry","seller","batch_id"],
    "/home/scraping/algo_scraping/FNAC/fnac_offers.csv": ["pfid","idsmartphone","url","timestamp","Price","shipcost","product_rating","seller","seller_rating","seller_sales_count","seller_rating_count","offertype","shipcountry","sellercountry","descriptsmartphone","batch_id"]
}


# Discord bot credentials (set these as environment variables)
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", 0))  # The channel ID where alerts will be sent

# Validate configuration
if not BOT_TOKEN:
    raise RuntimeError("DISCORD_BOT_TOKEN environment variable is not set.")
if CHANNEL_ID == 0:
    raise RuntimeError("DISCORD_CHANNEL_ID environment variable is not set or invalid.")

# -------- State Tracking --------
# Keep track of which alerts are currently active (path, column)
active_alerts: set[tuple[str, str]] = set()
# Keep track of which low price alerts have been sent (path, column, row index)
low_price_alerts: set[tuple[str, str, int]] = set()
monitor_task_started = False

# -------- Helper Functions --------
def format_error(message: str) -> str:
    """
    Format a message as an @everyone mention with bold Markdown for Discord.
    """
    return f"@everyone **🚨 ERREUR**: {message}"

def format_recovery(message: str) -> str:
    """
    Format a recovery message when an issue is resolved.
    """
    return f"✅ **RÉSOLU**: {message}"

def format_low_price(message: str) -> str:
    """
    Format a low-price alert message for Discord.
    """
    return f"ℹ️ **PRIX BAS**: {message}"

async def send_message(channel, content: str):
    """
    Send a message to Discord, catching exceptions.
    """
    try:
        await channel.send(content)
    except Exception as e:
        logging.error("Could not send message to Discord: %s", e)

def to_thread(func: typing.Callable) -> typing.Coroutine:
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)
    return wrapper

# -------- Monitoring Logic --------
@to_thread
def check_file(path: str, columns: list[str]) -> tuple[list[str], list[str]]:
    new_alerts: list[str] = []
    resolved_alerts: list[str] = []

    try:
        logging.info("Checking file: %s", path)
        df = pd.read_csv(path, low_memory=False)
    except Exception as e:
        key = (path, "__read_error__")
        msg = f"Impossible de lire `{path}`: {e}"
        if key not in active_alerts:
            new_alerts.append(format_error(msg))
            active_alerts.add(key)
        return new_alerts, resolved_alerts

    for col in columns:
        key = (path, col)
        if col not in df.columns:
            msg = f"Colonne `{col}` introuvable dans `{path}`."
            if key not in active_alerts:
                new_alerts.append(format_error(msg))
                active_alerts.add(key)
            continue
        
        # If file fnac and columns seller_rating	seller_sales_count	seller_rating_count, check the last 20 rows
        if path.endswith("fnac_offers.csv") and col in ["seller_rating", "seller_sales_count", "seller_rating_count", "product_rating"]:
            last_ten = df[col].tail(20)
        # If file amazon and columns rating, check the last 20 rows
        elif path.endswith("amazon_offers.csv") and col == "rating":
            last_ten = df[col].tail(20)
        else:
            last_ten = df[col].tail(10)

        # helper: null or empty/"N/A"
        def _is_empty(x):
            return pd.isnull(x) or (isinstance(x, str) and x.strip().upper() in ("", "N/A"))

        if last_ten.apply(_is_empty).all():
            msg = f"`{path}` → colonne `{col}` contient 10 valeurs consécutives nulles ou vides."
            if key not in active_alerts:
                new_alerts.append(format_error(msg))
                active_alerts.add(key)
        else:
            # If previously an alert existed, and now values are OK, mark resolved
            if key in active_alerts:
                resolved_alerts.append(format_recovery(f"`{path}` colonne `{col}`"))
                active_alerts.remove(key)

    return new_alerts, resolved_alerts

@to_thread
def check_low_prices(path: str, columns: list[str]) -> list[str]:
    """
    Checks for any prices under 50 € in the CSV at `path`, for columns named 'Price' or 'price'.
    Returns a list of formatted low-price alert messages, only for new occurrences.
    """
    new_msgs: list[str] = []
    try:
        # load only header + last 100 lines to save CPU/memory, but count total lines
        with open(path, 'r') as f:
            lines = f.readlines()
        total_lines = len(lines)                                  # get full file length
        header = lines[0]
        tail_lines = lines[-100:]
        tail_data = "".join(tail_lines)
        df = pd.read_csv(io.StringIO(header + tail_data))
        # compute offset for absolute data indices (exclude header)
        offset = (total_lines - 1) - len(tail_lines)
    except Exception:
        return new_msgs

    # helper to normalize strings like "759,00 €", "310€00" or "991.75" → float
    def _parse_price(x):
        try:
            s = str(x).strip()
            # direct parse for plain numeric values
            if re.fullmatch(r'\d+(\.\d+)?', s):
                return float(s)
            # match e.g. 310€00 or 759,00 €
            m = re.search(r'(\d+)[,\.\s]*€?(\d{2})', s)
            if m:
                return float(f"{m.group(1)}.{m.group(2)}")
            # fallback: strip non-numeric, convert commas
            s2 = re.sub(r'[^\d\.\,]', '', s)
            return float(s2.replace(',', '.'))
        except:
            return float('nan')

    for col in columns:
        if col.lower() == "price":
            prices = df[col].apply(_parse_price)
            mask = prices < 50
            if mask.any():
                rows = df.loc[mask]
                # Alert only on new occurrences
                for idx, row in rows.iterrows():
                    absolute_idx = offset + idx  + 1                    # absolute index in data
                    key = (path, col, int(absolute_idx))
                    if key in low_price_alerts:
                        continue
                    low_price_alerts.add(key)
                    url = row.get("url", "")
                    price = prices.iloc[idx]
                    product_name = row.get('Product Name', row.get('descriptsmartphone', row.get('idsmartphone', 'N/A')))
                    seller_name = row.get('Seller', row.get('seller', 'N/A'))
                    new_msgs.append(format_low_price(
                        f"`{path}` ligne {absolute_idx+1}: {col} = {price}€, "
                        f"model: {product_name}, seller: {seller_name}, url: {url if url else ''}"
                    ))

    # If no new alerts, return empty list
    if not new_msgs:
        return []
    # Limit to first 5 messages, then summarize additional ones
    if len(new_msgs) > 5:
        extra = len(new_msgs) - 5
        new_msgs = new_msgs[:5] + [format_low_price(f"`{path}`: {extra} autres nouveaux prix < 50 € détectés.")]
    return new_msgs

# New: check that the most recent timestamp is not older than 40 minutes
@to_thread
def check_timestamp(path: str, columns: list[str]) -> tuple[list[str], list[str]]:
    new_alerts: list[str] = []
    resolved_alerts: list[str] = []
    ts_cols = [c for c in columns if c.lower() == "timestamp"]
    if not ts_cols:
        return new_alerts, resolved_alerts
    col = ts_cols[0]
    try:
        df = pd.read_csv(path, usecols=[col], low_memory=False)
    except Exception:
        return new_alerts, resolved_alerts
    series = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
    if series.empty or series.isna().all():
        return new_alerts, resolved_alerts
    max_ts = series.max()
    now = datetime.datetime.now()
    key = (path, "__timestamp__")
    if (now - max_ts) > datetime.timedelta(minutes=45):
        msg = f"`{path}`: dernière mise à jour {max_ts}, > 45 minutes."
        if key not in active_alerts:
            new_alerts.append(format_error(msg))
            active_alerts.add(key)
    else:
        if key in active_alerts:
            resolved_alerts.append(format_recovery(f"`{path}` mis à jour récemment."))
            active_alerts.remove(key)
    return new_alerts, resolved_alerts

@tasks.loop(minutes=1)
async def monitor_loop():
    """Runs every 1 minute to check all configured CSV files."""
    channel = client.get_channel(CHANNEL_ID)
    if channel is None:
        logging.error("Could not find Discord channel with ID %s", CHANNEL_ID)
        return

    for path, cols in FILES_TO_MONITOR.items():
        new_alerts, resolved_alerts = await check_file(path, cols)
        # Send new error alerts
        for msg in new_alerts:
            await send_message(channel, msg) 
        # Send recovery messages
        for msg in resolved_alerts:
            await send_message(channel, msg)

        # Check for any low prices (< 50€) and send those alerts
        low_price_msgs = await check_low_prices(path, cols)
        for msg in low_price_msgs:
            await send_message(channel, msg)

        # New: check timestamp freshness
        ts_new, ts_res = await check_timestamp(path, cols)
        for msg in ts_new:
            await send_message(channel, msg)
        for msg in ts_res:
            await send_message(channel, msg)

@monitor_loop.before_loop
async def before_monitor():
    await client.wait_until_ready()
    logging.info("Logged in as %s, starting CSV monitoring task...", client.user)

# -------- Entry Point --------
if __name__ == "__main__":
    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        logging.info("Logged in as %s, starting CSV monitoring task...", client.user)
        global monitor_task_started
        if not monitor_task_started:
            monitor_loop.start()
            monitor_task_started = True

    client.run(BOT_TOKEN)
