import os
import time
import requests
import smtplib
import logging
import zipfile
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

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
GH_TOKEN        = os.environ["GH_TOKEN"]         # GitHub Personal Access Token
GH_REPO         = os.environ["GH_REPO"]          # e.g. "username/sebi-reg30-scraper"
CC_EMAILS       = os.environ.get("CC_EMAILS", "")   # comma-separated, optional

# ── Date Setup ────────────────────────────────────────────────
TODAY = datetime.now()

def get_fetch_dates():
    """
    Returns list of dates to fetch filings for.
    Monday: fetch Friday + Saturday + Sunday (3 days catch-up)
    Other days: fetch just yesterday
    """
    weekday = TODAY.weekday()  # 0=Monday, 4=Friday, 5=Saturday, 6=Sunday

    if weekday == 0:  # Monday
        dates = [TODAY - timedelta(days=d) for d in range(1, 4)]  # Sun, Sat, Fri
        log.info("📅 Monday detected — fetching Friday + Saturday + Sunday filings")
    else:
        dates = [TODAY - timedelta(days=1)]  # just yesterday
        log.info(f"📅 Fetching yesterday's filings")

    return dates

FETCH_DATES = get_fetch_dates()

# For labelling email/release — date range string
if len(FETCH_DATES) > 1:
    DATE_LABEL = f"{FETCH_DATES[-1].strftime('%Y-%m-%d')} to {FETCH_DATES[0].strftime('%Y-%m-%d')}"
else:
    DATE_LABEL = FETCH_DATES[0].strftime("%Y-%m-%d")

# ── Constants ─────────────────────────────────────────────────
MAX_EMAIL_SIZE = 25 * 1024 * 1024   # 25 MB in bytes

REG30_KEYWORDS = [
    "regulation 30", "reg 30", "reg. 30",
    "lodr", "material information", "material event",
]

PDF_DIR = f"pdfs/{TODAY.strftime('%Y-%m-%d')}"
os.makedirs(f"{PDF_DIR}/NSE", exist_ok=True)
os.makedirs(f"{PDF_DIR}/BSE", exist_ok=True)


# ── NSE/BSE Filter Constants ──────────────────────────────────

# NSE announcement categories that correspond to Reg 30 filings
NSE_REG30_CATEGORIES = [
    "company update",
    "board meeting",
    "outcome of board meeting",
    "press release",
    "analyst meet",
    "investor meet",
    "change in directors",
    "resignation of director",
    "appointment of director",
    "acquisition",
    "trading window",
]

# BSE category 7 = Company Update (Reg 30 filings)
BSE_REG30_CAT = "7"


# ── Helpers ───────────────────────────────────────────────────

def is_nse_reg30(filing):
    """
    NSE Reg 30 filings — filter by 'desc' field which contains the category.
    From actual API: {"desc": "Analysts/Institutional Investor Meet/Con. Call Updates", ...}
    smIndustry is null — use desc for category matching.
    Also check attchmntText for regulation 30 mentions.
    """
    desc         = (filing.get("desc", "") or "").lower()
    attach_text  = (filing.get("attchmntText", "") or "").lower()

    return (
        any(cat in desc for cat in NSE_REG30_CATEGORIES)
        or "reg 30" in desc
        or "regulation 30" in desc
        or "reg 30" in attach_text
        or "regulation 30" in attach_text
        or "lodr" in attach_text
    )


def get_total_size(pdf_files):
    total = 0
    for f in pdf_files:
        if f and os.path.exists(f):
            total += os.path.getsize(f)
    return total


def safe_filename(name, max_len=60):
    return name.replace("/", "-").replace("\\", "-").replace(":", "-")[:max_len]


# ── BSE ───────────────────────────────────────────────────────

BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.bseindia.com/",
    "Accept": "application/json",
}


def fetch_bse_filings(date):
    date_str = date.strftime("%Y%m%d")
    label    = date.strftime("%Y-%m-%d")
    log.info(f"📡 Fetching BSE filings for {label}...")

    # Try category 7 (Company Update / Reg 30) first, fallback to all (-1)
    all_filings = []
    for cat in [BSE_REG30_CAT, "-1"]:
        url = (
            "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
            f"?strCat={cat}&strPrevDate={date_str}&strScrip=&strSearch="
            f"&strToDate={date_str}&strType=C&subcategory=-1"
        )
        try:
            resp = requests.get(url, headers=BSE_HEADERS, timeout=30)
            resp.raise_for_status()
            filings = resp.json().get("Table", [])
            if filings:
                log.info(f"   BSE total announcements (cat={cat}): {len(filings)}")
                all_filings = filings
                break
        except Exception as e:
            log.error(f"   BSE fetch error (cat={cat}): {e}")
            continue

    if not all_filings:
        log.warning(f"   BSE returned 0 announcements for {label}")
    return all_filings


def filter_bse_reg30(filings):
    """
    BSE category 7 already filters to Company Update / Reg 30 filings.
    When fetching all (-1), filter by subject keywords.
    """
    reg30 = []
    for f in filings:
        subject  = (f.get("NEWSSUB", "") or f.get("HEADLINE", "") or "").lower()
        category = (f.get("CATEGORYNAME", "") or f.get("SUBCATNAME", "") or "").lower()
        # Include if category matches OR subject mentions regulation 30
        if (
            "company update" in category
            or "reg 30" in subject
            or "regulation 30" in subject
            or "lodr" in subject
            or "material" in subject
        ):
            reg30.append({
                "exchange": "BSE",
                "company":  f.get("SLONGNAME", "Unknown"),
                "subject":  f.get("NEWSSUB", "") or f.get("HEADLINE", ""),
                "news_id":  f.get("NEWSID", ""),
                "pdf_name": f.get("ATTACHMENTNAME", ""),
            })
    log.info(f"   BSE Reg 30 filings: {len(reg30)}")
    return reg30


def download_bse_pdf(filing):
    company  = safe_filename(filing["company"])
    news_id  = filing["news_id"]
    pdf_name = filing["pdf_name"]

    if not pdf_name:
        log.warning(f"   No attachment for BSE: {company}")
        return None

    url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{pdf_name}"
    try:
        resp = requests.get(url, headers=BSE_HEADERS, timeout=30)
        resp.raise_for_status()
        filename = f"{PDF_DIR}/BSE/{company}_{news_id}.pdf"
        with open(filename, "wb") as f:
            f.write(resp.content)
        log.info(f"   ✅ BSE: {company}")
        return filename
    except Exception as e:
        log.error(f"   ❌ BSE PDF failed for {company}: {e}")
        return None


# ── NSE ───────────────────────────────────────────────────────

NSE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept":          "*/*",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer":         "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-origin",
    "priority":        "u=1, i",
}


def get_nse_session():
    """
    NSE requires a valid session with cookies.
    Must visit homepage + announcements page first to get cookies.
    """
    session = requests.Session()
    try:
        # Step 1: Hit homepage to get initial cookies
        session.get(
            "https://www.nseindia.com",
            headers=NSE_HEADERS,
            timeout=15
        )
        time.sleep(2)

        # Step 2: Hit the exact referer page used in the curl request
        session.get(
            "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            headers=NSE_HEADERS,
            timeout=15
        )
        time.sleep(2)
        log.info("   NSE session established ✅")
    except Exception as e:
        log.error(f"   NSE session error: {e}")
    return session


def fetch_nse_filings(session, date):
    """
    Fetch NSE filings for both equities and SME indexes.
    Uses exact API format observed from NSE website.
    """
    date_str = date.strftime("%d-%m-%Y")
    label    = date.strftime("%Y-%m-%d")
    log.info(f"📡 Fetching NSE filings for {label}...")

    all_filings = []

    for index in ["equities", "sme"]:
        url = (
            "https://www.nseindia.com/api/corporate-announcements"
            f"?index={index}&from_date={date_str}&to_date={date_str}&reqXbrl=false"
        )
        try:
            resp = session.get(url, headers=NSE_HEADERS, timeout=30)
            resp.raise_for_status()
            filings = resp.json()
            if isinstance(filings, list):
                log.info(f"   NSE {index} announcements: {len(filings)}")
                all_filings.extend(filings)
            else:
                log.warning(f"   NSE {index} unexpected response: {filings}")
        except Exception as e:
            log.error(f"   NSE {index} fetch error: {e}")
        time.sleep(1)

    log.info(f"   NSE total announcements: {len(all_filings)}")
    return all_filings


def filter_nse_reg30(filings):
    reg30 = []
    for f in filings:
        if is_nse_reg30(f):
            reg30.append({
                "exchange": "NSE",
                "company":  f.get("sm_name", "") or f.get("comp", "Unknown"),
                "subject":  f.get("desc", "") or f.get("subject", ""),
                "symbol":   f.get("symbol", ""),
                "attchmnt": f.get("attchmntFile", "") or f.get("attchmnt", ""),
                "an_no":    f.get("seq_id", "") or f.get("an_no", "") or f.get("sort_date", "").replace(" ", "_").replace(":", "-"),
            })
    log.info(f"   NSE Reg 30 filings: {len(reg30)}")
    return reg30


def download_nse_pdf(session, filing):
    company  = safe_filename(filing["company"])
    attchmnt = filing["attchmnt"]
    an_no    = filing["an_no"] or "unknown"

    if not attchmnt:
        log.warning(f"   No attachment for NSE: {company}")
        return None

    # attchmntFile is already a full URL (https://nsearchives.nseindia.com/...)
    url = attchmnt if attchmnt.startswith("http") else f"https://www.nseindia.com{attchmnt}"
    try:
        resp = session.get(url, headers=NSE_HEADERS, timeout=30)
        resp.raise_for_status()
        filename = f"{PDF_DIR}/NSE/{company}_{an_no}.pdf"
        with open(filename, "wb") as f:
            f.write(resp.content)
        log.info(f"   ✅ NSE: {company}")
        return filename
    except Exception as e:
        log.error(f"   ❌ NSE PDF failed for {company}: {e}")
        return None


# ── GitHub Release ────────────────────────────────────────────

def create_github_release(pdf_files):
    """
    Create a GitHub Release tagged with the date and upload all PDFs to it.
    Returns the release URL.
    """
    log.info("☁️  Uploading to GitHub Release...")

    headers = {
        "Authorization": f"token {GH_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Step 1: Create the release
    # Tag must be URL-safe — replace spaces and special chars
    safe_tag = DATE_LABEL.replace(" ", "-").replace("—", "to")
    release_payload = {
        "tag_name":   safe_tag,
        "name":       f"SEBI Reg 30 Filings — {DATE_LABEL}",
        "body":       f"Automated Reg 30 filings from NSE + BSE for {DATE_LABEL}.",
        "draft":      False,
        "prerelease": False,
    }

    try:
        resp = requests.post(
            f"https://api.github.com/repos/{GH_REPO}/releases",
            json=release_payload,
            headers=headers,
            timeout=30
        )
        resp.raise_for_status()
        release = resp.json()
        upload_url   = release["upload_url"].replace("{?name,label}", "")
        release_url  = release["html_url"]
        log.info(f"   ✅ Release created: {release_url}")
    except Exception as e:
        log.error(f"   ❌ Failed to create GitHub Release: {e}")
        return None

    # Step 2: Upload each PDF to the release
    uploaded_names = set()
    for idx, pdf_path in enumerate(pdf_files):
        if not pdf_path or not os.path.exists(pdf_path):
            continue
        filename = os.path.basename(pdf_path)
        # Handle duplicate filenames by adding index
        if filename in uploaded_names:
            name, ext = os.path.splitext(filename)
            filename = f"{name}_{idx}{ext}"
        uploaded_names.add(filename)
        # URL encode the filename
        encoded_name = requests.utils.quote(filename)
        try:
            with open(pdf_path, "rb") as f:
                upload_resp = requests.post(
                    f"{upload_url}?name={encoded_name}",
                    headers={
                        **headers,
                        "Content-Type": "application/octet-stream",
                    },
                    data=f,
                    timeout=60
                )
            upload_resp.raise_for_status()
            log.info(f"   ☁️  Uploaded: {filename}")
        except Exception as e:
            log.error(f"   ❌ Upload failed for {filename}: {e}")

    return release_url


# ── Email ─────────────────────────────────────────────────────

def send_email_with_attachments(pdf_files, bse_count, nse_count):
    """Send email with PDFs directly attached."""
    total   = len(pdf_files)
    subject = f"SEBI Reg 30 Filings — {DATE_LABEL} | {total} PDFs (BSE: {bse_count}, NSE: {nse_count})"
    body    = f"""Dear User,

Please find attached all SEBI Regulation 30 (LODR) filings declared to NSE and BSE on {DATE_LABEL}.

Summary:
  • BSE Filings : {bse_count}
  • NSE Filings : {nse_count}
  • Total PDFs  : {total}

Regards,
SEBI Reg 30 Auto-Scraper"""

    msg = MIMEMultipart()
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = RECEIVER_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    for pdf_path in pdf_files:
        if not pdf_path or not os.path.exists(pdf_path):
            continue
        try:
            with open(pdf_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(pdf_path)}"
            )
            msg.attach(part)
        except Exception as e:
            log.error(f"   Could not attach {pdf_path}: {e}")

    _send_smtp(msg)


def send_email_with_link(release_url, bse_count, nse_count, total_pdfs, total_mb):
    """Send email with GitHub Release download link (when PDFs exceed 25MB)."""
    subject = f"SEBI Reg 30 Filings — {DATE_LABEL} | {total_pdfs} PDFs (BSE: {bse_count}, NSE: {nse_count})"
    body    = f"""Dear User,

SEBI Regulation 30 (LODR) filings for {DATE_LABEL} are ready.

Total size ({total_mb:.1f} MB) exceeded the 25MB email limit.
All PDFs have been uploaded to GitHub — download them here:

👉 {release_url}

Summary:
  • BSE Filings : {bse_count}
  • NSE Filings : {nse_count}
  • Total PDFs  : {total_pdfs}
  • Total Size  : {total_mb:.1f} MB

Regards,
SEBI Reg 30 Auto-Scraper"""

    msg = MIMEMultipart()
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = RECEIVER_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    _send_smtp(msg)


def _send_smtp(msg):
    """Send email via Outlook SMTP."""
    # Build recipient list — To + CC
    cc_list  = [e.strip() for e in CC_EMAILS.split(",") if e.strip()]
    all_rcpt = [RECEIVER_EMAIL] + cc_list

    if cc_list:
        msg["Cc"] = ", ".join(cc_list)

    try:
        # with smtplib.SMTP("smtp.office365.com", 587) as server:
            # server.starttls()
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

    all_pdfs  = []
    bse_count = 0
    nse_count = 0

    # ── Loop over all fetch dates (1 day normally, 3 days on Monday) ──
    nse_session = get_nse_session()

    for fetch_date in FETCH_DATES:
        day_label = fetch_date.strftime("%Y-%m-%d")
        log.info(f"\n📅 Processing: {day_label}")

        # BSE
        bse_filings = fetch_bse_filings(fetch_date)
        bse_reg30   = filter_bse_reg30(bse_filings)
        bse_count  += len(bse_reg30)
        for filing in bse_reg30:
            pdf = download_bse_pdf(filing)
            if pdf:
                all_pdfs.append(pdf)
            time.sleep(1)

        # NSE
        nse_filings = fetch_nse_filings(nse_session, fetch_date)
        nse_reg30   = filter_nse_reg30(nse_filings)
        nse_count  += len(nse_reg30)
        for filing in nse_reg30:
            pdf = download_nse_pdf(nse_session, filing)
            if pdf:
                all_pdfs.append(pdf)
            time.sleep(1)

    log.info(f"\n📊 BSE={bse_count} | NSE={nse_count} | PDFs downloaded={len(all_pdfs)}")

    if not all_pdfs:
        log.info("⚪ No Reg 30 filings found. No email sent.")
        return

    # ── Check total size ──
    total_size = get_total_size(all_pdfs)
    total_mb   = total_size / (1024 * 1024)
    log.info(f"📦 Total PDF size: {total_mb:.1f} MB")

    if total_size <= MAX_EMAIL_SIZE:
        # ✅ Under 25MB — attach directly
        log.info("📧 Size OK — sending PDFs as attachments...")
        send_email_with_attachments(all_pdfs, bse_count, nse_count)
    else:
        # ⚠️ Over 25MB — upload to GitHub Release, email the link
        log.info(f"⚠️  Size {total_mb:.1f}MB exceeds 25MB — uploading to GitHub Release...")
        release_url = create_github_release(all_pdfs)
        if release_url:
            send_email_with_link(release_url, bse_count, nse_count, len(all_pdfs), total_mb)
        else:
            log.error("GitHub Release failed — sending email without PDFs.")
            send_email_with_link(
                "GitHub Release creation failed — check Actions logs.",
                bse_count, nse_count, len(all_pdfs), total_mb
            )

    log.info("✅ Done!")


if __name__ == "__main__":
    main()