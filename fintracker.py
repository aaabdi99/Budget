"""Minimal Gmail → Supabase ingestion script for Fintracker."""
from __future__ import annotations

import argparse
import base64
import html
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from email.utils import parsedate_to_datetime
from typing import Iterable, List, Optional

from dateutil import parser as date_parser
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from supabase import Client, create_client
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
GOOGLE_CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
GOOGLE_TOKEN_PATH = os.path.join(DATA_DIR, "token.json")
KEYS_PATH = os.path.join(BASE_DIR, "keys.json")

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
EASTERN = ZoneInfo("America/New_York")
EST_SUFFIX = " EST"

SUPPORTED_SENDERS = {
    "capitalone.com": "capital_one"

}

SUPPORTED_SUBJECTS = {
    "Capital One": "capital_one"

}

ALLOWED_SENDER_EMAILS = {
    "capitalone@notification.capitalone.com"
}

AmountRegex = re.compile(r"\$([0-9,]+\.?[0-9]{0,2})")
CAPITAL_ONE_VENDOR_REGEX = re.compile(
    r"\bat\s+([^\n,]+?)(?=(?:,|\swith|\sfor|\son|$))",
    re.I,
)
BOA_VENDOR_REGEX = re.compile(
    r"\bat\s+([^\n,]+?)(?=(?:\s+on|,|\swith|\sfor|$))",
    re.I,
)

AUTO_EXCLUDE_PLACES = {
    "DOMINION ENERGY",
    "NEW RAINBOW TEXTILES L",
    "NEW RAINBOW TEXTILES LTD",
    "SPI*FAIRFAX WATER BILL",
    "VERIZON BILL PAYMENT",
}

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RawEmail:
    id: str
    subject: str
    sender: str
    received_at: datetime
    body_plain: str
    body_html: Optional[str] = None


@dataclass(frozen=True)
class ParsedTransaction:
    vendor: str
    amount: float
    timestamp: datetime


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def ensure_est_12h(value: datetime | str) -> str:
    if isinstance(value, str):
        dt = date_parser.parse(value)
    elif isinstance(value, datetime):
        dt = value
    else:
        raise TypeError("value must be a datetime or string")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    dt_est = dt.astimezone(EASTERN)
    milliseconds = dt_est.microsecond // 1000
    date_part = dt_est.strftime("%Y-%m-%d %I:%M:%S")
    am_pm = dt_est.strftime("%p")
    return f"{date_part}.{milliseconds:03d} {am_pm} EST"


def is_auto_excluded_vendor(vendor: Optional[str]) -> bool:
    if vendor is None:
        return False
    return vendor.strip().upper() in AUTO_EXCLUDE_PLACES


def _load_credentials() -> Credentials:
    if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
        raise FileNotFoundError("Missing credentials.json")

    creds: Optional[Credentials] = None
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(GOOGLE_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_PATH, GMAIL_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(GOOGLE_CREDENTIALS_PATH, scopes=GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
        with open(GOOGLE_TOKEN_PATH, "w") as fh:
            fh.write(creds.to_json())
    return creds


_SUPABASE_CLIENT: Optional[Client] = None


def _supabase() -> Client:
    global _SUPABASE_CLIENT
    if _SUPABASE_CLIENT is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY")
        if (not url or not key) and os.path.exists(KEYS_PATH):
            with open(KEYS_PATH, "r") as fh:
                config = json.load(fh)
            url = url or config.get("SUPABASE_URL")
            key = key or config.get("SUPABASE_SERVICE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")
        _SUPABASE_CLIENT = create_client(url, key)
    return _SUPABASE_CLIENT


# ---------------------------------------------------------------------------
# Gmail client
# ---------------------------------------------------------------------------


def _gmail_service():
    creds = _load_credentials()
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _parse_message_payload(message: dict) -> tuple[str, str, datetime, str, Optional[str]]:
    payload = message.get("payload", {})
    headers = payload.get("headers", [])
    header_lookup = {h.get("name", "").lower(): h.get("value", "") for h in headers}

    subject = header_lookup.get("subject", "")
    sender = header_lookup.get("from", "")
    received_raw = header_lookup.get("date")
    if not received_raw:
        raise ValueError("Email message missing Date header")

    received_dt = parsedate_to_datetime(received_raw)
    if received_dt.tzinfo is None:
        received_dt = received_dt.replace(tzinfo=timezone.utc)

    parts = payload.get("parts", [])
    body_plain = ""
    body_html = None

    if payload.get("mimeType") == "text/plain" and payload.get("body", {}).get("data"):
        body_plain = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="ignore")
    else:
        for part in parts:
            mime_type = part.get("mimeType")
            data = part.get("body", {}).get("data")
            if not data:
                continue
            decoded = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
            if mime_type == "text/plain" and not body_plain:
                body_plain = decoded
            elif mime_type == "text/html" and not body_html:
                body_html = decoded

    return subject, sender, received_dt, body_plain, body_html


def fetch_raw_emails(
    after: Optional[datetime] = None,
    label_ids: Optional[Iterable[str]] = None,
    max_results: Optional[int] = 200,
) -> List[RawEmail]:
    service = _gmail_service()

    query = ""
    if after:
        query = f"after:{int(after.timestamp())}"

    remaining = None if max_results is None or max_results <= 0 else max_results

    def _page_size() -> int:
        if remaining is None:
            return 500
        return max(1, min(remaining, 500))

    list_kwargs: dict = {"userId": "me"}
    if query:
        list_kwargs["q"] = query
    if label_ids:
        list_kwargs["labelIds"] = list(label_ids)

    messages: List[RawEmail] = []
    next_page: Optional[str] = None

    while True:
        page_kwargs = dict(list_kwargs)
        page_kwargs["maxResults"] = _page_size()
        if next_page:
            page_kwargs["pageToken"] = next_page

        response = service.users().messages().list(**page_kwargs).execute()
        summaries = response.get("messages", []) or []
        if not summaries:
            break

        for summary in summaries:
            message = (
                service.users()
                .messages()
                .get(userId="me", id=summary["id"], format="full")
                .execute()
            )
            subject, sender, received_dt, body_plain, body_html = _parse_message_payload(message)
            messages.append(
                RawEmail(
                    id=summary["id"],
                    subject=subject,
                    sender=sender,
                    received_at=received_dt,
                    body_plain=body_plain,
                    body_html=body_html,
                )
            )

            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    return messages

        next_page = response.get("nextPageToken")
        if not next_page:
            break

    return messages


# ---------------------------------------------------------------------------
# Email parsing helpers
# ---------------------------------------------------------------------------


def _strip_html(raw_html: Optional[str]) -> str:
    if not raw_html:
        return ""
    text = re.sub(r"<[^>]+>", " ", raw_html)
    text = html.unescape(text)
    return text.replace("\xa0", " ")


def _text_candidates(email: RawEmail) -> List[str]:
    return [email.body_plain or "", _strip_html(email.body_html), email.subject or ""]


def _first_match(regex: re.Pattern[str], email: RawEmail) -> Optional[str]:
    for text in _text_candidates(email):
        if not text:
            continue
        match = regex.search(text)
        if match:
            return match.group(1).strip()
    return None


def _fallback_vendor(email: RawEmail) -> Optional[str]:
    for text in _text_candidates(email):
        if not text:
            continue
        match = re.search(r"\bat\s+([^,\n]+),", text, re.I)
        if match:
            return match.group(1).strip()
    return None


def _normalize_vendor(vendor: str) -> str:
    vendor = re.split(r"\b(with|for|on)\b", vendor, maxsplit=1)[0].strip()
    return vendor.strip(" .,")


def parse_capital_one(email: RawEmail) -> Optional[ParsedTransaction]:
    amount_str = _first_match(AmountRegex, email)
    vendor_str = _first_match(CAPITAL_ONE_VENDOR_REGEX, email) or _fallback_vendor(email)
    if not amount_str or not vendor_str:
        return None
    return ParsedTransaction(
        vendor=_normalize_vendor(vendor_str),
        amount=float(amount_str.replace(",", "")),
        timestamp=email.received_at,
    )


def parse_bank_of_america(email: RawEmail) -> Optional[ParsedTransaction]:
    amount_str = _first_match(AmountRegex, email)
    vendor_str = _first_match(BOA_VENDOR_REGEX, email)
    if not amount_str or not vendor_str:
        for text in _text_candidates(email):
            if not text:
                continue
            match = re.search(r"purchase at\s+(.+)\s+for", text, re.I)
            if match:
                vendor_str = match.group(1).strip()
                break
    if not amount_str or not vendor_str:
        return None
    return ParsedTransaction(
        vendor=_normalize_vendor(vendor_str),
        amount=float(amount_str.replace(",", "")),
        timestamp=email.received_at,
    )


def parse_generic(email: RawEmail) -> Optional[ParsedTransaction]:
    amount_match = _first_match(AmountRegex, email)
    if not amount_match:
        return None

    combined = "\n".join(filter(None, _text_candidates(email)))
    lines = combined.splitlines()
    vendor_str = None
    for idx, line in enumerate(lines):
        if AmountRegex.search(line):
            if idx - 1 >= 0:
                vendor_str = lines[idx - 1].strip()
            elif idx + 1 < len(lines):
                vendor_str = lines[idx + 1].strip()
            break
    if not vendor_str:
        vendor_str = email.subject.strip() or _fallback_vendor(email)
    if not vendor_str:
        return None

    return ParsedTransaction(
        vendor=_normalize_vendor(vendor_str),
        amount=float(amount_match.replace(",", "")),
        timestamp=email.received_at,
    )


PARSERS = {
    "capital_one": parse_capital_one,
    "bank_of_america": parse_bank_of_america,
}


def parse_email(email: RawEmail) -> Optional[ParsedTransaction]:
    sender_lower = email.sender.lower()
    sender_email = sender_lower.split("<")[-1].split(">")[0].strip()
    allowed_sender = sender_email in ALLOWED_SENDER_EMAILS or any(
        sender_lower.endswith(domain) for domain in SUPPORTED_SENDERS.keys()
    )
    if not allowed_sender:
        return None
    for domain, key in SUPPORTED_SENDERS.items():
        if domain in sender_lower:
            parser = PARSERS.get(key, parse_generic)
            return parser(email)

    for keyword, key in SUPPORTED_SUBJECTS.items():
        if keyword.lower() in email.subject.lower():
            parser = PARSERS.get(key, parse_generic)
            return parser(email)

    return parse_generic(email)


# ---------------------------------------------------------------------------
# Supabase operations
# ---------------------------------------------------------------------------


def _transaction_exists(client: Client, datetime_est: str, place: str, cost: float) -> bool:
    response = (
        client.table("transactions")
        .select("id")
        .eq("datetime_est", datetime_est)
        .eq("place", place)
        .eq("cost", cost)
        .limit(1)
        .execute()
    )
    return bool(response.data)


def insert_transactions(transactions: Iterable[ParsedTransaction]) -> int:
    client = _supabase()
    inserted = 0
    for tx in transactions:
        datetime_est = ensure_est_12h(tx.timestamp)
        place = tx.vendor.strip()
        cost = float(tx.amount)
        if not place:
            continue
        if _transaction_exists(client, datetime_est, place, cost):
            continue
        include_flag = not is_auto_excluded_vendor(place)
        client.table("transactions").insert(
            {
                "datetime_est": datetime_est,
                "place": place,
                "cost": cost,
                "include_flag": include_flag,
            }
        ).execute()
        inserted += 1
    return inserted


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


def add_all(max_results: Optional[int]) -> None:
    emails = fetch_raw_emails(max_results=max_results)
    parsed: List[ParsedTransaction] = []
    for email in emails:
        parsed_tx = parse_email(email)
        if parsed_tx:
            parsed.append(parsed_tx)

    inserted = insert_transactions(parsed)
    print("=== Fintracker Ingestion Summary ===")
    print(f"Emails fetched : {len(emails)}")
    print(f"Parsed         : {len(parsed)}")
    print(f"Inserted       : {inserted}")


def add_after(timestamp_str: str, max_results: Optional[int]) -> None:
    after_dt = date_parser.parse(timestamp_str)
    if after_dt.tzinfo is None:
        after_dt = after_dt.replace(tzinfo=timezone.utc)
    emails = fetch_raw_emails(after=after_dt, max_results=max_results)
    parsed = [tx for email in emails if (tx := parse_email(email))]
    inserted = insert_transactions(parsed)
    print("=== Fintracker Ingestion Summary (addAfter) ===")
    print(f"After          : {timestamp_str}")
    print(f"Emails fetched : {len(emails)}")
    print(f"Parsed         : {len(parsed)}")
    print(f"Inserted       : {inserted}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fintracker Supabase ingestion CLI")
    parser.add_argument(
        "command",
        choices=["addAll", "addAfter"],
        nargs="?",
        default="addAll",
        help="addAll runs Gmail ingestion; addAfter fetches emails after a given timestamp.",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=200,
        help="Maximum number of emails to fetch when running addAll (0 for unlimited).",
    )
    parser.add_argument(
        "--after",
        type=str,
        help="Timestamp string for addAfter (e.g., '2025-11-16 08:29:06.000 PM EST').",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.command == "addAfter":
        if not args.after:
            raise SystemExit("addAfter requires --after \"TIMESTAMP\"")
        add_after(args.after, args.max_results)
    else:
        add_all(args.max_results)


if __name__ == "__main__":
    main()
