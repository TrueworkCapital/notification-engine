import os
import io
import time
import zipfile
import logging
import smtplib
import requests
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config (from GitHub Secrets) ─────────────────────────────
SENDER_EMAIL    = os.environ["SENDER_EMAIL"]
SENDER_PASSWORD = os.environ["SENDER_PASSWORD"]
RECEIVER_EMAIL  = os.environ["RECEIVER_EMAIL"]
CC_EMAILS       = os.environ.get("CC_EMAILS", "")
GH_TOKEN        = os.environ["GH_TOKEN"]
GH_REPO         = os.environ["GH_REPO"]   # e.g. "TrueworkCapital/notification-engine"

# ── Date Setup ────────────────────────────────────────────────
TODAY = datetime.now()

def get_fetch_dates():
    weekday = TODAY.weekday()  # 0=Monday
    if weekday == 0:
        dates = [TODAY - timedelta(days=d) for d in range(1, 4)]
        log.info("📅 Monday — fetching Friday + Saturday + Sunday")
    else:
        dates = [TODAY - timedelta(days=1)]
        log.info("📅 Fetching yesterday's filings")
    return dates

FETCH_DATES = get_fetch_dates()

if len(FETCH_DATES) > 1:
    DATE_LABEL = f"{FETCH_DATES[-1].strftime('%Y-%m-%d')}_to_{FETCH_DATES[0].strftime('%Y-%m-%d')}"
else:
    DATE_LABEL = FETCH_DATES[0].strftime("%Y-%m-%d")

# Safe tag for GitHub Release
RELEASE_TAG = f"sebi-reg30-{DATE_LABEL}"
ZIP_FILENAME = f"SEBI_Reg30_{DATE_LABEL}.zip"

# ── NSE Config ────────────────────────────────────────────────
NSE_SUBJECT = "Analysts/Institutional Investor Meet/Con. Call Updates"

NSE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept":          "*/*",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer":         "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-origin",
    "priority":        "u=1, i",
    "sec-gpc":         "1",
}


# ── NSE Session ───────────────────────────────────────────────

def get_nse_session():
    session = requests.Session()
    try:
        session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=15)
        time.sleep(2)
        session.get(
            "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            headers=NSE_HEADERS, timeout=15
        )
        time.sleep(2)
        log.info("   NSE session established ✅")
    except Exception as e:
        log.error(f"   NSE session error: {e}")
    return session


# ── NSE Fetch ─────────────────────────────────────────────────

def fetch_nse_filings(session, date):
    date_str = date.strftime("%d-%m-%Y")
    label    = date.strftime("%Y-%m-%d")
    log.info(f"📡 Fetching NSE filings for {label}...")

    all_filings = []
    for index in ["equities", "sme"]:
        url = (
            "https://www.nseindia.com/api/corporate-announcements"
            f"?index={index}"
            f"&from_date={date_str}"
            f"&to_date={date_str}"
            f"&reqXbrl=false"
            f"&subject={requests.utils.quote(NSE_SUBJECT)}"
        )
        try:
            resp = session.get(url, headers=NSE_HEADERS, timeout=30)
            resp.raise_for_status()
            filings = resp.json()
            if isinstance(filings, list):
                log.info(f"   NSE {index}: {len(filings)} filings")
                all_filings.extend(filings)
            else:
                log.warning(f"   NSE {index} unexpected response: {filings}")
        except Exception as e:
            log.error(f"   NSE {index} error: {e}")
        time.sleep(1)

    log.info(f"   NSE total: {len(all_filings)} filings")
    return all_filings


def parse_filings(filings):
    parsed = []
    for f in filings:
        pdf_url = f.get("attchmntFile", "") or f.get("attchmnt", "")
        if not pdf_url:
            continue
        parsed.append({
            "company": f.get("sm_name", "") or f.get("comp", "Unknown"),
            "symbol":  f.get("symbol", ""),
            "pdf_url": pdf_url if pdf_url.startswith("http") else f"https://www.nseindia.com{pdf_url}",
        })
    return parsed


def download_pdf(session, url, company):
    try:
        resp = session.get(url, headers=NSE_HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        log.error(f"   ❌ Download failed for {company}: {e}")
        return None


# ── ZIP ───────────────────────────────────────────────────────

def create_zip(pdf_map):
    """
    Create a ZIP file in memory from a dict of {filename: pdf_bytes}.
    Returns ZIP bytes.
    """
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, pdf_bytes in pdf_map.items():
            zf.writestr(filename, pdf_bytes)
    zip_buffer.seek(0)
    return zip_buffer.read()


# ── GitHub Release ────────────────────────────────────────────

GH_HEADERS = {
    "Authorization": f"token {GH_TOKEN}",
    "Accept":        "application/vnd.github.v3+json",
}

def delete_existing_release(tag):
    """Delete release + tag if already exists (avoid 422 on re-run)."""
    resp = requests.get(
        f"https://api.github.com/repos/{GH_REPO}/releases/tags/{tag}",
        headers=GH_HEADERS, timeout=15
    )
    if resp.status_code == 200:
        release_id = resp.json()["id"]
        requests.delete(
            f"https://api.github.com/repos/{GH_REPO}/releases/{release_id}",
            headers=GH_HEADERS, timeout=15
        )
        requests.delete(
            f"https://api.github.com/repos/{GH_REPO}/git/refs/tags/{tag}",
            headers=GH_HEADERS, timeout=15
        )
        log.info(f"   🗑️  Deleted existing release: {tag}")


def upload_to_github_release(zip_bytes, total_count):
    """
    Create a GitHub Release and upload the ZIP file.
    Returns the release URL.
    """
    log.info("☁️  Uploading ZIP to GitHub Release...")

    # Delete existing release for same tag (safe re-run)
    delete_existing_release(RELEASE_TAG)

    # Create release
    release_payload = {
        "tag_name":   RELEASE_TAG,
        "name":       f"SEBI Reg 30 — {DATE_LABEL} ({total_count} filings)",
        "body":       f"NSE Reg 30 filings for {DATE_LABEL}.\nTotal: {total_count} PDFs zipped.",
        "draft":      False,
        "prerelease": False,
    }

    try:
        resp = requests.post(
            f"https://api.github.com/repos/{GH_REPO}/releases",
            json=release_payload,
            headers=GH_HEADERS,
            timeout=30
        )
        resp.raise_for_status()
        release      = resp.json()
        upload_url   = release["upload_url"].replace("{?name,label}", "")
        release_url  = release["html_url"]
        log.info(f"   ✅ Release created: {release_url}")
    except Exception as e:
        log.error(f"   ❌ Failed to create release: {e}")
        return None

    # Upload ZIP as single asset
    try:
        upload_resp = requests.post(
            f"{upload_url}?name={ZIP_FILENAME}",
            headers={
                **GH_HEADERS,
                "Content-Type": "application/zip",
            },
            data=zip_bytes,
            timeout=120
        )
        upload_resp.raise_for_status()
        log.info(f"   ✅ ZIP uploaded: {ZIP_FILENAME} ({len(zip_bytes)/1024/1024:.1f} MB)")
    except Exception as e:
        log.error(f"   ❌ ZIP upload failed: {e}")

    return release_url


# ── Email ─────────────────────────────────────────────────────

def send_email(release_url, total_count, zip_size_mb):
    subject = f"Daily Filings Update — {DATE_LABEL} | {total_count} Documents"
    
    body = f"""Dear Team,

The filings for {DATE_LABEL} are now available.

📥 Download all {total_count} documents (ZIP — {zip_size_mb:.1f} MB):
{release_url}

Steps to access:
  1. Open the link above
  2. Under "Assets", click "{ZIP_FILENAME}"
  3. Extract the ZIP file to view all documents

Summary:
  • Total Documents : {total_count}
  • Source          : NSE (Equities + SME)
  • Category        : Analysts/Institutional Investor Meet

Note: The download link will remain available for 3 days.

Regards,  
Automated Notifications
"""

    msg = MIMEMultipart()
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = RECEIVER_EMAIL
    msg["Subject"] = subject

    cc_list = [e.strip() for e in CC_EMAILS.split(",") if e.strip()]
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    all_rcpt = [RECEIVER_EMAIL] + cc_list

    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, all_rcpt, msg.as_string())
        log.info(f"   ✅ Email sent to {RECEIVER_EMAIL}" + (f" + CC: {', '.join(cc_list)}" if cc_list else ""))
    except Exception as e:
        log.error(f"   ❌ Email failed: {e}")
        raise


# ── Main ──────────────────────────────────────────────────────

def main():
    log.info(f"🚀 SEBI Reg 30 Scraper — {DATE_LABEL}")
    log.info("=" * 55)

    nse_session = get_nse_session()
    pdf_map     = {}   # filename → pdf_bytes

    for fetch_date in FETCH_DATES:
        label = fetch_date.strftime("%Y-%m-%d")
        log.info(f"\n📅 Processing: {label}")

        raw     = fetch_nse_filings(nse_session, fetch_date)
        filings = parse_filings(raw)
        log.info(f"   {len(filings)} filings with PDFs")

        for idx, filing in enumerate(filings, 1):
            company = filing["company"]
            symbol  = filing["symbol"]
            pdf_url = filing["pdf_url"]

            # Safe unique filename inside ZIP
            safe_co  = company.replace("/", "-").replace("\\", "-").replace(":", "-")[:50].strip()
            filename = f"{safe_co}_{symbol}_{idx}.pdf"

            log.info(f"   [{idx}/{len(filings)}] Downloading: {company}")
            pdf_bytes = download_pdf(nse_session, pdf_url, company)
            if pdf_bytes:
                pdf_map[filename] = pdf_bytes

            time.sleep(0.5)

    total = len(pdf_map)
    log.info(f"\n📊 Total PDFs downloaded: {total}")

    if total == 0:
        log.info("⚪ No filings found. No email sent.")
        return

    # Create ZIP in memory
    log.info(f"🗜️  Creating ZIP with {total} PDFs...")
    zip_bytes   = create_zip(pdf_map)
    zip_size_mb = len(zip_bytes) / (1024 * 1024)
    log.info(f"   ZIP size: {zip_size_mb:.1f} MB")

    # Upload ZIP to GitHub Release
    release_url = upload_to_github_release(zip_bytes, total)
    if not release_url:
        log.error("❌ GitHub Release failed. Exiting.")
        return

    # Email the link
    log.info("📧 Sending email...")
    send_email(release_url, total, zip_size_mb)

    log.info("✅ Done!")


if __name__ == "__main__":
    main()