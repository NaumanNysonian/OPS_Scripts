#!/usr/bin/env python3
import argparse
import csv
import base64
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

GRAPHQL_URL = "https://public-api.shiphero.com/graphql"
DEFAULT_CUSTOMER_ACCOUNT_ID = "QWNjb3VudDo4MjQ0OQ=="

CSV_SHIPMENTS_FIELDS = [
  "shipment_id",
  "shipment_uuid",
  "warehouse",
  "warehouse_id",
  "warehouse_uuid",
  "partner_order_id",
  "order_number",
  "tracking_number",
  "custom_tracking_url",
  "package_number",
  "total_packages",
  "shipping_method",
  "shipping_carrier",
  "completed",
  "created_at",
  "order_uuid",
  "order_gift_note",
]

CSV_ADDRESS_FIELDS = [
  "shipment_id",
  "name",
  "address1",
  "address2",
  "city",
  "state",
  "country",
  "zip",
  "created_at",
]

CSV_LINE_ITEM_FIELDS = [
  "shipment_id",
  "line_item_id",
  "sku",
  "quantity",
]

GRAPHQL_QUERY = """
query Shipments($customer_account_id: String, $date_from: ISODateTime, $date_to: ISODateTime, $after: String) {
  shipments(customer_account_id: $customer_account_id, date_from: $date_from, date_to: $date_to) {
    request_id
    complexity
    data(first: 100, after: $after) {
      pageInfo { hasNextPage endCursor }
      edges {
        cursor
        node {
          id
          legacy_id
          order_id
          user_id
          warehouse_id
          created_date
          order { order_number }
          line_items(first: 10) {
            edges { node { line_item_id quantity line_item { sku } } }
          }
          shipped_off_shiphero
          delivered
          picked_up
          refunded
          needs_refund
          address { name address1 address2 city state country zip }
          shipping_labels {
            tracking_number
            carrier
            shipping_method
            order_number
            cost
          }
        }
      }
    }
  }
}
"""


def get_utc_now():
  # Current UTC timestamp for consistent logging and filenames.
  return datetime.now(timezone.utc)


def format_iso_utc(dt):
  # Normalize datetime to ISO8601 UTC format with Z suffix.
  if dt.tzinfo is None:
    dt = dt.replace(tzinfo=timezone.utc)
  return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class UTCFormatter(logging.Formatter):
  def formatTime(self, record, datefmt=None):
    dt = datetime.fromtimestamp(record.created, timezone.utc)
    if datefmt:
      return dt.strftime(datefmt)
    return dt.isoformat().replace("+00:00", "Z")


LOG = logging.getLogger("shiphero_shipments")


def configure_logging(log_dir=None, log_level=None):
  if LOG.handlers:
    return LOG

  level_name = (log_level or os.environ.get("SHIPHERO_LOG_LEVEL") or os.environ.get("LOG_LEVEL") or "INFO").upper()
  level = getattr(logging, level_name, logging.INFO)
  LOG.setLevel(level)
  LOG.propagate = False

  formatter = UTCFormatter("[%(asctime)s] %(levelname)s: %(message)s", "%Y-%m-%dT%H:%M:%S.%fZ")

  console_handler = logging.StreamHandler(sys.stdout)
  console_handler.setFormatter(formatter)
  console_handler.setLevel(level)
  LOG.addHandler(console_handler)

  if not log_dir:
    log_dir = os.environ.get("SHIPHERO_LOG_DIR") or os.environ.get("LOG_DIR")
  if not log_dir:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, "logs")

  try:
    os.makedirs(log_dir, exist_ok=True)
    log_date = get_utc_now().strftime("%Y-%m-%d")
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    log_path = os.path.join(log_dir, f"{script_name}.{log_date}.log")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    LOG.addHandler(file_handler)
    LOG.info("Logging to %s", log_path)
  except Exception as exc:
    LOG.warning("Failed to initialize file logging: %s", exc)

  return LOG


def load_env_file(path=".env"):
  # Load environment variables from a local .env file.
  if not os.path.exists(path):
    return
  try:
    with open(path, "r", encoding="utf-8") as f:
      for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
          continue
        if "=" not in line:
          continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
          os.environ.setdefault(key, value)
  except Exception as exc:
    raise RuntimeError(f"Failed to load .env file: {exc}")


def decode_gid_numeric_id(gid):
  # Decode base64 GID like "Shipment:733368298" and return numeric ID.
  if not gid:
    return None
  try:
    decoded = base64.b64decode(str(gid)).decode("utf-8")
    if ":" in decoded:
      tail = decoded.split(":")[-1].strip()
      if tail.isdigit():
        return int(tail)
  except Exception:
    return None
  return None


def build_carrier_tracking_url(carrier, tracking_number):
  # Construct carrier tracking URL when supported.
  if not carrier or not tracking_number:
    return None
  c = str(carrier).lower()
  tn = str(tracking_number)
  if c == "ups":
    return f"http://wwwapps.ups.com/WebTracking/track?track=yes&trackNums={requests.utils.quote(tn)}"
  if c in ("fedex", "fed_ex"):
    return f"https://www.fedex.com/fedextrack/?trknbr={requests.utils.quote(tn)}"
  if c == "usps":
    return f"https://tools.usps.com/go/TrackConfirmAction?tLabels={requests.utils.quote(tn)}"
  return None


def require_env(name, required=True, default=None):
  # Fetch environment variable with optional required enforcement.
  val = os.environ.get(name, default)
  if required and not val:
    raise RuntimeError(f"Missing required environment variable: {name}")
  return val


def open_postgres_connection():
  # Open Postgres connection for DB sync or token lookup.
  # Prefer DATABASE_URL if provided, otherwise fall back to PG* variables.
  try:
    import psycopg2 as pg_driver
  except Exception:
    try:
      import psycopg as pg_driver
    except Exception as exc:
      raise RuntimeError("Postgres driver missing. Install psycopg[binary] or psycopg2-binary.") from exc

  database_url = os.environ.get("DATABASE_URL")
  if database_url:
    return pg_driver.connect(database_url)

  return pg_driver.connect(
    host=require_env("PGHOST"),
    port=int(require_env("PGPORT", required=False, default="5432")),
    dbname=require_env("PGDATABASE"),
    user=require_env("PGUSER"),
    password=require_env("PGPASSWORD"),
    sslmode=os.environ.get("PGSSLMODE", "prefer"),
  )


def fetch_shiphero_token_from_db(cur, token_name):
  # Resolve ShipHero token from the tokens table.
  sql = """
  SELECT token_value
  FROM public.third_party_tokens
  WHERE token_name = %s
  ORDER BY token_inserted_at DESC
  LIMIT 1;
  """
  cur.execute(sql, (token_name,))
  row = cur.fetchone()
  token = row[0] if row else None
  if not token:
    raise RuntimeError(f"Missing ShipHero token for token_name={token_name}")
  return token


def open_csv_outputs(csv_dir):
  # Create CSV writers and files for shipments, addresses, and line items.
  os.makedirs(csv_dir, exist_ok=True)
  ts = get_utc_now().strftime("%Y%m%dT%H%M%SZ")

  shipments_path = os.path.join(csv_dir, f"shipments_{ts}.csv")
  addresses_path = os.path.join(csv_dir, f"shipment_addresses_{ts}.csv")
  line_items_path = os.path.join(csv_dir, f"shipment_line_items_{ts}.csv")

  f_ship = open(shipments_path, "w", newline="", encoding="utf-8")
  f_addr = open(addresses_path, "w", newline="", encoding="utf-8")
  f_line = open(line_items_path, "w", newline="", encoding="utf-8")

  ship_writer = csv.DictWriter(f_ship, fieldnames=CSV_SHIPMENTS_FIELDS)
  addr_writer = csv.DictWriter(f_addr, fieldnames=CSV_ADDRESS_FIELDS)
  line_writer = csv.DictWriter(f_line, fieldnames=CSV_LINE_ITEM_FIELDS)

  ship_writer.writeheader()
  addr_writer.writeheader()
  line_writer.writeheader()

  return {
    "shipments": ship_writer,
    "addresses": addr_writer,
    "line_items": line_writer,
    "files": [f_ship, f_addr, f_line],
    "paths": {
      "shipments": shipments_path,
      "addresses": addresses_path,
      "line_items": line_items_path,
    },
  }


def close_csv_outputs(writers):
  # Close CSV file handles safely.
  if not writers:
    return
  for f in writers.get("files", []):
    try:
      f.close()
    except Exception:
      pass


def write_csv_rows(writers, shipment):
  # Write normalized shipment to CSV outputs.
  if not writers:
    return

  writers["shipments"].writerow({
    "shipment_id": shipment.get("shipment_id"),
    "shipment_uuid": shipment.get("shipment_uuid"),
    "warehouse": shipment.get("warehouse"),
    "warehouse_id": shipment.get("warehouse_id"),
    "warehouse_uuid": shipment.get("warehouse_uuid"),
    "partner_order_id": shipment.get("partner_order_id"),
    "order_number": shipment.get("order_number"),
    "tracking_number": shipment.get("tracking_number"),
    "custom_tracking_url": shipment.get("custom_tracking_url"),
    "package_number": shipment.get("package_number"),
    "total_packages": shipment.get("total_packages"),
    "shipping_method": shipment.get("shipping_method"),
    "shipping_carrier": shipment.get("shipping_carrier"),
    "completed": shipment.get("completed"),
    "created_at": shipment.get("created_at"),
    "order_uuid": shipment.get("order_uuid"),
    "order_gift_note": shipment.get("order_gift_note"),
  })

  address = shipment.get("address") or {}
  if address:
    writers["addresses"].writerow({
      "shipment_id": shipment.get("shipment_id"),
      "name": address.get("name"),
      "address1": address.get("address1"),
      "address2": address.get("address2"),
      "city": address.get("city"),
      "state": address.get("state"),
      "country": address.get("country"),
      "zip": address.get("zip"),
      "created_at": shipment.get("created_at"),
    })

  li_edges = (shipment.get("line_items") or {}).get("edges") or []
  if isinstance(li_edges, list):
    for li in li_edges:
      li_node = (li or {}).get("node") or {}
      sku = None
      if li_node.get("line_item"):
        sku = li_node["line_item"].get("sku")
      if sku is None:
        continue
      writers["line_items"].writerow({
        "shipment_id": shipment.get("shipment_id"),
        "line_item_id": li_node.get("line_item_id"),
        "sku": sku,
        "quantity": li_node.get("quantity"),
      })


def fetch_shiphero_shipments_page(token, customer_account_id, date_from, date_to, after=None, timeout=120):
  # Call ShipHero GraphQL API for a single page of shipments.
  headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
  }
  payload = {
    "query": GRAPHQL_QUERY,
    "variables": {
      "customer_account_id": customer_account_id,
      "date_from": date_from,
      "date_to": date_to,
      "after": after,
    },
  }
  resp = requests.post(GRAPHQL_URL, headers=headers, json=payload, timeout=timeout)
  try:
    resp.raise_for_status()
  except requests.HTTPError as exc:
    detail = resp.text.strip()
    raise RuntimeError(f"ShipHero HTTP {resp.status_code}: {detail}") from exc
  data = resp.json()
  if "errors" in data and data["errors"]:
    raise RuntimeError(f"GraphQL errors: {data['errors']}")
  return data


def upsert_shipment_row(cur, shipment):
  # Insert shipment row or update existing by shipment_id.
  sql = """
  INSERT INTO store.orders_shipment (
    shipment_id,
    shipment_uuid,
    warehouse,
    warehouse_id,
    warehouse_uuid,
    partner_order_id,
    order_number,
    tracking_number,
    custom_tracking_url,
    package_number,
    total_packages,
    shipping_method,
    shipping_carrier,
    completed,
    created_at,
    order_uuid,
    order_gift_note,
    system_datetime
  ) VALUES (
    %s, %s,
    %s, %s, %s,
    %s,
    %s,
    %s,
    %s,
    %s, %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    now()
  )
  ON CONFLICT (shipment_id) DO UPDATE SET
    shipment_uuid       = COALESCE(EXCLUDED.shipment_uuid, store.orders_shipment.shipment_uuid),
    warehouse           = COALESCE(EXCLUDED.warehouse, store.orders_shipment.warehouse),
    warehouse_id        = COALESCE(EXCLUDED.warehouse_id, store.orders_shipment.warehouse_id),
    warehouse_uuid      = COALESCE(EXCLUDED.warehouse_uuid, store.orders_shipment.warehouse_uuid),
    partner_order_id    = COALESCE(EXCLUDED.partner_order_id, store.orders_shipment.partner_order_id),
    order_number        = COALESCE(EXCLUDED.order_number, store.orders_shipment.order_number),
    tracking_number     = COALESCE(EXCLUDED.tracking_number, store.orders_shipment.tracking_number),
    custom_tracking_url = COALESCE(EXCLUDED.custom_tracking_url, store.orders_shipment.custom_tracking_url),
    package_number      = COALESCE(EXCLUDED.package_number, store.orders_shipment.package_number),
    total_packages      = COALESCE(EXCLUDED.total_packages, store.orders_shipment.total_packages),
    shipping_method     = COALESCE(EXCLUDED.shipping_method, store.orders_shipment.shipping_method),
    shipping_carrier    = COALESCE(EXCLUDED.shipping_carrier, store.orders_shipment.shipping_carrier),
    completed           = COALESCE(EXCLUDED.completed, store.orders_shipment.completed),
    created_at          = COALESCE(EXCLUDED.created_at, store.orders_shipment.created_at),
    order_uuid          = COALESCE(EXCLUDED.order_uuid, store.orders_shipment.order_uuid),
    order_gift_note     = COALESCE(EXCLUDED.order_gift_note, store.orders_shipment.order_gift_note),
    system_datetime     = now()
  RETURNING id;
  """
  cur.execute(sql, (
    shipment["shipment_id"],
    shipment["shipment_uuid"],
    shipment["warehouse"],
    shipment["warehouse_id"],
    shipment["warehouse_uuid"],
    shipment["partner_order_id"],
    shipment["order_number"],
    shipment["tracking_number"],
    shipment["custom_tracking_url"],
    shipment["package_number"],
    shipment["total_packages"],
    shipment["shipping_method"],
    shipment["shipping_carrier"],
    shipment["completed"],
    shipment["created_at"],
    shipment["order_uuid"],
    shipment["order_gift_note"],
  ))
  row = cur.fetchone()
  return row[0] if row else None


def insert_shipping_address_if_missing(cur, shipment_db_id, address, created_at):
  # Insert shipping address once per shipment.
  if not address:
    return
  sql = """
  INSERT INTO store.orders_shipment_shipping_address (
    shipment_id,
    package_id,
    name,
    address1,
    address2,
    address_city,
    address_zip,
    address_state,
    address_country,
    created_at
  )
  SELECT %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s
  WHERE NOT EXISTS (
    SELECT 1 FROM store.orders_shipment_shipping_address a
    WHERE a.shipment_id = %s
  );
  """
  cur.execute(sql, (
    shipment_db_id,
    address.get("name"),
    address.get("address1"),
    address.get("address2"),
    address.get("city"),
    address.get("zip"),
    address.get("state"),
    address.get("country"),
    created_at,
    shipment_db_id,
  ))


def upsert_line_item_row(cur, shipment_db_id, sku, quantity):
  # Insert or update shipment line item.
  sql = """
  INSERT INTO store.orders_shipment_line_item (shipment_id, sku, quantity)
  VALUES (%s, %s, %s)
  ON CONFLICT (shipment_id, sku)
  DO UPDATE SET quantity = EXCLUDED.quantity;
  """
  cur.execute(sql, (shipment_db_id, sku, quantity))


def normalize_shipment_record(node):
  # Normalize ShipHero shipment payload to internal fields.
  label = None
  if isinstance(node.get("shipping_labels"), list) and node["shipping_labels"]:
    label = node["shipping_labels"][0]
  elif isinstance(node.get("shipping_labels"), dict):
    label = node["shipping_labels"]
  else:
    label = {}

  raw_order_number = None
  if node.get("order"):
    raw_order_number = node["order"].get("order_number")
  clean_order_number = None
  if raw_order_number is not None:
    clean_order_number = str(raw_order_number).lstrip("#")

  legacy_id = node.get("legacy_id")
  shipment_id = None
  if legacy_id is not None:
    try:
      shipment_id = int(legacy_id)
    except Exception:
      shipment_id = None
  if shipment_id is None:
    shipment_id = decode_gid_numeric_id(node.get("id"))

  warehouse_id = decode_gid_numeric_id(node.get("warehouse_id"))

  tracking_number = label.get("tracking_number")
  carrier_raw = label.get("carrier")

  return {
    "shipment_id": shipment_id,
    "shipment_uuid": node.get("id"),
    "warehouse": "Primary" if warehouse_id == 110091 else None,
    "warehouse_id": warehouse_id,
    "warehouse_uuid": node.get("warehouse_id"),
    "partner_order_id": None,
    "order_number": clean_order_number,
    "tracking_number": tracking_number,
    "custom_tracking_url": build_carrier_tracking_url(carrier_raw, tracking_number),
    "package_number": 1,
    "total_packages": 1,
    "shipping_method": label.get("shipping_method"),
    "shipping_carrier": str(carrier_raw).upper() if carrier_raw else None,
    "completed": bool(tracking_number) if tracking_number is not None else None,
    "created_at": node.get("created_date"),
    "order_uuid": node.get("order_id"),
    "order_gift_note": None,
    "address": node.get("address") or {},
    "line_items": node.get("line_items") or {},
  }


def main():
  # Load .env before parsing CLI defaults that depend on environment variables.
  load_env_file()

  parser = argparse.ArgumentParser(description="Sync ShipHero shipments into Postgres")
  parser.add_argument("--date-from", dest="date_from", help="ISO8601 datetime (UTC)")
  parser.add_argument("--date-to", dest="date_to", help="ISO8601 datetime (UTC)")
  parser.add_argument("--window-minutes", dest="window_minutes", type=int, default=10)
  parser.add_argument("--sleep-seconds", dest="sleep_seconds", type=int, default=4)
  parser.add_argument("--csv-dir", dest="csv_dir", help="Directory to write CSVs")
  parser.add_argument("--csv-only", dest="csv_only", action="store_true", help="Write CSVs only (skip DB)")
  parser.add_argument("--customer-account-id", dest="customer_account_id", help="ShipHero customer account ID")
  parser.add_argument("--token-name", dest="token_name", default=os.environ.get("SHIPHERO_TOKEN_NAME", "Shiphero_Nysonian"))
  parser.add_argument("--log-dir", dest="log_dir", help="Directory to write log files")
  parser.add_argument("--log-level", dest="log_level", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
  args = parser.parse_args()

  configure_logging(args.log_dir, args.log_level)

  # Resolve the customer account scope for the ShipHero API.
  customer_account_id = (
    args.customer_account_id
    or os.environ.get("SHIPHERO_CUSTOMER_ACCOUNT_ID")
    or DEFAULT_CUSTOMER_ACCOUNT_ID
  )
  if not customer_account_id:
    raise RuntimeError("Missing customer account id (set SHIPHERO_CUSTOMER_ACCOUNT_ID or --customer-account-id)")

  # Resolve the time window in UTC.
  if args.date_to:
    date_to = args.date_to
  else:
    date_to = format_iso_utc(get_utc_now())

  if args.date_from:
    date_from = args.date_from
  else:
    date_from = format_iso_utc(get_utc_now() - timedelta(minutes=args.window_minutes))

  # Prepare CSV outputs if requested.
  csv_dir = args.csv_dir or (os.getcwd() if args.csv_only else None)
  writers = open_csv_outputs(csv_dir) if csv_dir else None
  if writers:
    paths = writers.get("paths", {})
    LOG.info(
      "Writing CSVs: %s, %s, %s",
      paths.get("shipments"),
      paths.get("addresses"),
      paths.get("line_items"),
    )

  # Prefer explicit token from env; fallback to DB lookup.
  token = os.environ.get("SHIPHERO_TOKEN")
  db_enabled = not args.csv_only
  conn = None
  close_after_token = False

  try:
    if db_enabled:
      conn = open_postgres_connection()
      conn.autocommit = False

    if not token:
      if conn is None:
        conn = open_postgres_connection()
        conn.autocommit = False
        close_after_token = True
      with conn.cursor() as cur:
        token = fetch_shiphero_token_from_db(cur, args.token_name)
      if close_after_token:
        conn.close()
        conn = None

    LOG.info("Sync start: %s -> %s (customer_account_id=%s)", date_from, date_to, customer_account_id)

    # Paginate through ShipHero shipments.
    cursor = None
    page = 1
    total_shipments = 0
    while True:
      data = fetch_shiphero_shipments_page(token, customer_account_id, date_from, date_to, after=cursor)
      shipments_data = data.get("data", {}).get("shipments", {}).get("data")
      if not shipments_data:
        raise RuntimeError("ShipHero response missing data.shipments.data")

      page_info = shipments_data.get("pageInfo") or {}
      edges = shipments_data.get("edges") or []

      if db_enabled:
        with conn.cursor() as cur:
          for edge in edges:
            node = (edge or {}).get("node") or {}
            shipment = normalize_shipment_record(node)

            if shipment["shipment_id"] is None:
              LOG.warning("Skipping shipment with missing shipment_id: %s", node.get("id"))
              continue

            if writers:
              write_csv_rows(writers, shipment)

            shipment_db_id = upsert_shipment_row(cur, shipment)
            if not shipment_db_id:
              LOG.warning("Skipping shipment insert (no db id): %s", shipment.get("shipment_id"))
              continue

            insert_shipping_address_if_missing(
              cur,
              shipment_db_id,
              shipment.get("address") or {},
              shipment.get("created_at"),
            )

            li_edges = (shipment.get("line_items") or {}).get("edges") or []
            if isinstance(li_edges, list):
              for li in li_edges:
                li_node = (li or {}).get("node") or {}
                sku = None
                if li_node.get("line_item"):
                  sku = li_node["line_item"].get("sku")
                quantity = li_node.get("quantity")
                if sku is None:
                  continue
                upsert_line_item_row(cur, shipment_db_id, sku, quantity)

            total_shipments += 1

          conn.commit()
      else:
        for edge in edges:
          node = (edge or {}).get("node") or {}
          shipment = normalize_shipment_record(node)

          if shipment["shipment_id"] is None:
            LOG.warning("Skipping shipment with missing shipment_id: %s", node.get("id"))
            continue

          if writers:
            write_csv_rows(writers, shipment)

          total_shipments += 1

      has_next = bool(page_info.get("hasNextPage"))
      cursor = page_info.get("endCursor")
      LOG.info("Page %s: %s shipments, hasNextPage=%s", page, len(edges), has_next)

      if not has_next:
        break

      page += 1
      time.sleep(args.sleep_seconds)

    LOG.info("Sync done. Total shipments processed: %s", total_shipments)
  finally:
    close_csv_outputs(writers)
    if conn is not None:
      conn.close()


if __name__ == "__main__":
  try:
    main()
  except Exception as exc:
    if not LOG.handlers:
      configure_logging()
    LOG.exception("Unhandled error: %s", exc)
    sys.exit(1)
