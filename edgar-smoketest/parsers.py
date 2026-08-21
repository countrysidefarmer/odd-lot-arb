"""Parsing for EDGAR daily indexes, Form 4 ownership XML, and SC 13D headers/body."""
import re
import xml.etree.ElementTree as ET

# The 2026 daily index labels 13Ds as "SCHEDULE 13D[/A]"; the legacy "SC 13D[/A]"
# code still appears for a handful. Accept both, normalise downstream.
KEEP_FORMS = {"4", "SC 13D", "SC 13D/A", "SCHEDULE 13D", "SCHEDULE 13D/A"}
ARCHIVES = "https://www.sec.gov/Archives/"


def normalize_form(form_type):
    """Collapse label variants to: '4', 'SC 13D' (original), 'SC 13D/A' (amend)."""
    ft = form_type.strip().upper()
    if ft == "4":
        return "4"
    if ft.endswith("13D/A"):
        return "SC 13D/A"
    if ft.endswith("13D"):
        return "SC 13D"
    return form_type


# ---------------------------------------------------------------------------
# Daily form index (fixed-width)
# ---------------------------------------------------------------------------
# Data rows are fixed-width, but the header line is wrapped across two lines in
# these files, so we anchor on the trailing three fields instead of the header:
#   <left = form type + company>  <CIK digits>  <YYYYMMDD>  <edgar/...txt>
_ROW_RE = re.compile(
    r"^(?P<left>.+?)\s{2,}(?P<cik>\d+)\s+(?P<date>\d{8})\s+(?P<file>edgar/\S+\.txt)\s*$")


def parse_form_idx(text):
    """Parse a form.YYYYMMDD.idx. Returns list of dicts for KEEP_FORMS only."""
    rows = []
    for line in text.splitlines():
        m = _ROW_RE.match(line)
        if not m:
            continue
        left = m.group("left").rstrip()
        # form type is the leading field, separated from company by >=2 spaces
        parts = re.split(r"\s{2,}", left, maxsplit=1)
        form_type = parts[0].strip()
        if form_type not in KEEP_FORMS:
            continue
        company = parts[1].strip() if len(parts) > 1 else ""
        cik = m.group("cik")
        date_filed = m.group("date")
        file_name = m.group("file")
        accession = file_name.rsplit("/", 1)[-1]
        if accession.endswith(".txt"):
            accession = accession[:-4]
        rows.append({
            "form_type": form_type,
            "form_class": normalize_form(form_type),
            "company": company,
            "cik": cik,
            "date_filed": date_filed,
            "file_name": file_name,
            "accession": accession,
            "url": ARCHIVES + file_name,
        })
    return rows


# ---------------------------------------------------------------------------
# Form 4 ownership XML
# ---------------------------------------------------------------------------
_OWNERSHIP_RE = re.compile(r"<ownershipDocument>.*?</ownershipDocument>", re.DOTALL)


def extract_ownership_xml(sgml_text):
    m = _OWNERSHIP_RE.search(sgml_text)
    return m.group(0) if m else None


def _text(el):
    return el.text.strip() if el is not None and el.text else None


def _val(parent, path):
    """Get text at path; unwrap a nested <value> child if present."""
    el = parent.find(path)
    if el is None:
        return None
    v = el.find("value")
    if v is not None:
        return _text(v)
    return _text(el)


def _flag(parent, path):
    raw = _val(parent, path)
    if raw is None:
        return None
    return raw.strip().lower() in ("1", "true", "y", "yes")


def parse_form4(xml_text):
    """Parse ownershipDocument XML into a dict. Returns None on XML failure."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None

    issuer = root.find("issuer")
    rec = {
        "issuerCik": _val(issuer, "issuerCik") if issuer is not None else None,
        "issuerName": _val(issuer, "issuerName") if issuer is not None else None,
        "issuerTradingSymbol": _val(issuer, "issuerTradingSymbol") if issuer is not None else None,
        "rptOwnerCik": None,
        "rptOwnerName": None,
        "isDirector": None,
        "isOfficer": None,
        "isTenPercentOwner": None,
        "officerTitle": None,
        "transactions": [],
    }

    owner = root.find("reportingOwner")
    if owner is not None:
        rec["rptOwnerCik"] = _val(owner, "reportingOwnerId/rptOwnerCik")
        rec["rptOwnerName"] = _val(owner, "reportingOwnerId/rptOwnerName")
        rel = owner.find("reportingOwnerRelationship")
        if rel is not None:
            rec["isDirector"] = _flag(rel, "isDirector")
            rec["isOfficer"] = _flag(rel, "isOfficer")
            rec["isTenPercentOwner"] = _flag(rel, "isTenPercentOwner")
            rec["officerTitle"] = _val(rel, "officerTitle")

    for t in root.findall(".//nonDerivativeTransaction"):
        rec["transactions"].append({
            "transactionDate": _val(t, "transactionDate"),
            "transactionCode": _val(t, "transactionCoding/transactionCode"),
            "transactionShares": _val(t, "transactionAmounts/transactionShares"),
            "transactionPricePerShare": _val(t, "transactionAmounts/transactionPricePerShare"),
            "acquiredDisposedCode": _val(t, "transactionAmounts/transactionAcquiredDisposedCode"),
            "sharesOwnedFollowingTransaction": _val(
                t, "postTransactionAmounts/sharesOwnedFollowingTransaction"),
        })
    return rec


# ---------------------------------------------------------------------------
# 10b5-1 detection (element name discovered empirically; see probe_10b5.py)
# ---------------------------------------------------------------------------
_FOOTNOTE_10B5_RE = re.compile(r"10b5[\-\s]?1", re.IGNORECASE)

# Discovered empirically via probe_10b5.py (see sample_form4.xml): the mandatory
# post-2023-04-01 checkbox is <aff10b5One> with values 0/1/true/false.
TENB51_ELEMENTS = ["aff10b5One"]


def detect_10b5_1(xml_text, element_names):
    """Return (flag, source). flag: True/False/None.

    element_names: list of candidate XML tag names carrying the checkbox value.
    Falls back to footnote text regex if no element present.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None, "xml-error"

    for name in element_names:
        for el in root.iter(name):
            raw = None
            v = el.find("value")
            if v is not None and v.text:
                raw = v.text
            elif el.text:
                raw = el.text
            if raw is not None:
                return (raw.strip().lower() in ("1", "true", "y", "yes")), "element"

    # Fallback: footnote text
    for fn in root.iter("footnote"):
        if fn.text and _FOOTNOTE_10B5_RE.search(fn.text):
            return True, "footnote"
    return None, "absent"


# ---------------------------------------------------------------------------
# SC 13D  (SGML header + best-effort body regex)
# ---------------------------------------------------------------------------
def _header_block(text, marker):
    """Return the slice of the SGML header under a top-level marker
    (e.g. 'SUBJECT COMPANY:' or 'FILED BY:'), up to the next such marker."""
    idx = text.find(marker)
    if idx == -1:
        return None
    rest = text[idx + len(marker):]
    # stop at the next top-level section marker
    stops = ["SUBJECT COMPANY:", "FILED BY:", "FILER:", "<DOCUMENT>"]
    end = len(rest)
    for s in stops:
        j = rest.find(s)
        if j != -1:
            end = min(end, j)
    return rest[:end]


def _grab(block, label):
    if block is None:
        return None
    m = re.search(re.escape(label) + r":\s*(.+)", block)
    return m.group(1).strip() if m else None


_CUSIP_RE = re.compile(r"CUSIP\s*(?:No\.?|Number|#)?\s*[:\-]?\s*([0-9A-Z]{6,9})", re.IGNORECASE)
_PCT_RE = re.compile(
    r"PERCENT OF CLASS REPRESENTED BY AMOUNT IN ROW\s*\(?11\)?[^\d]*([\d]{1,3}(?:\.\d+)?)\s*%?",
    re.IGNORECASE | re.DOTALL)


# Since Dec 2024 most SC 13D/G filings carry a structured XML primary document
# (namespace http://www.sec.gov/edgar/schedule13D) inside an <edgarSubmission>
# envelope. Parse that when present; fall back to legacy plain-text regex.
_XML_ENVELOPE_RE = re.compile(r"<edgarSubmission\b.*?</edgarSubmission>", re.DOTALL)


def _localname(tag):
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _lfind(el, name):
    for c in el.iter():
        if _localname(c.tag) == name:
            return c
    return None


def _lfindall(el, name):
    return [c for c in el.iter() if _localname(c.tag) == name]


def _ltext(el, name):
    x = _lfind(el, name)
    if x is not None and x.text and x.text.strip():
        return x.text.strip()
    return None


def parse_sc13d(sgml_text):
    header_end = sgml_text.find("<DOCUMENT>")
    header = sgml_text[:header_end] if header_end != -1 else sgml_text[:8000]

    subj = _header_block(header, "SUBJECT COMPANY:")
    filer = _header_block(header, "FILED BY:")
    if filer is None:
        filer = _header_block(header, "FILER:")

    rec = {
        "accession": _grab(header, "ACCESSION NUMBER"),
        "form_type": _grab(header, "CONFORMED SUBMISSION TYPE"),
        "date_filed": _grab(header, "FILED AS OF DATE"),
        "subject_name": _grab(subj, "COMPANY CONFORMED NAME"),
        "subject_cik": _grab(subj, "CENTRAL INDEX KEY"),
        "filer_name": _grab(filer, "COMPANY CONFORMED NAME"),
        "filer_cik": _grab(filer, "CENTRAL INDEX KEY"),
        "cusip": None,
        "pct_of_class": None,
        "item4": None,
        "parse_mode": None,
    }

    m = _XML_ENVELOPE_RE.search(sgml_text)
    if m:
        try:
            root = ET.fromstring(m.group(0))
        except ET.ParseError:
            root = None
        if root is not None:
            rec["parse_mode"] = "structured"
            rec["cusip"] = _ltext(root, "issuerCusipNumber") or _ltext(root, "cusipNumber")
            # row (11) percent per reporting person -> keep the largest stake
            pcts = []
            for el in _lfindall(root, "percentOfClass"):
                if el.text:
                    try:
                        pcts.append(float(el.text.strip().rstrip("%")))
                    except ValueError:
                        pass
            if pcts:
                rec["pct_of_class"] = f"{max(pcts):g}"
            item4 = _lfind(root, "item4")
            if item4 is not None:
                txt = " ".join(t.strip() for t in item4.itertext() if t.strip())
                txt = _WS_RE.sub(" ", txt).strip()
                rec["item4"] = txt[:1500] or None
            if not rec["subject_name"]:
                rec["subject_name"] = _ltext(root, "issuerName")

    # Legacy plain-text fallback (pre-2024 style filings)
    if rec["cusip"] is None and rec["item4"] is None:
        body = sgml_text[header_end:] if header_end != -1 else sgml_text
        body_text = _strip_tags(body)
        mm = _CUSIP_RE.search(body_text)
        if mm:
            rec["cusip"] = mm.group(1).strip()
        mm = _PCT_RE.search(body_text)
        if mm:
            rec["pct_of_class"] = mm.group(1).strip()
        rec["item4"] = _extract_item4(body_text)
        rec["parse_mode"] = rec["parse_mode"] or "legacy-regex"

    return rec


_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]+")


def _strip_tags(s):
    s = _TAG_RE.sub(" ", s)
    s = s.replace("&nbsp;", " ").replace("&#160;", " ")
    s = _WS_RE.sub(" ", s)
    return s


_ITEM4_START = re.compile(r"Item\s*4\.?\s*[\.\-:]?\s*Purpose of (?:the )?Transaction", re.IGNORECASE)
_ITEM5_START = re.compile(r"Item\s*5\.?\s*[\.\-:]?\s*Interest", re.IGNORECASE)


def _extract_item4(body_text):
    m = _ITEM4_START.search(body_text)
    if not m:
        return None
    start = m.end()
    tail = body_text[start:start + 8000]
    m5 = _ITEM5_START.search(tail)
    section = tail[:m5.start()] if m5 else tail
    section = section.strip()
    return section[:1500] if section else None
