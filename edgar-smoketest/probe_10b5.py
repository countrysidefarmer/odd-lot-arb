"""Empirically discover the 10b5-1 checkbox element in Form 4 ownership XML.
Fetches N recent Form 4s, dumps one raw XML, greps element names + footnotes."""
import re
import json
import xml.etree.ElementTree as ET
from collections import Counter

from edgar_client import TokenBucket, EdgarClient
from parsers import extract_ownership_xml

BASE = "/Users/thomasfielden/claude-projects/edgar-smoketest"


def main(n=20):
    with open(f"{BASE}/filings_5d.json") as f:
        rows = [r for r in json.load(f) if r["form_class"] == "4"]
    rows = rows[:n]

    bucket = TokenBucket(rate=10, capacity=10)
    client = EdgarClient(bucket, cache_dir=f"{BASE}/cache")

    all_tags = Counter()
    tags_with_10b5 = Counter()      # element names literally containing '10b5'
    context_tags = Counter()        # element names whose text mentions '10b5'
    footnote_hits = 0
    parsed = 0
    dumped = False

    for r in rows:
        text, _ = client.fetch(r["url"], f"{BASE}/cache/{r['accession']}.txt")
        if text is None:
            continue
        xml = extract_ownership_xml(text)
        if not xml:
            continue
        if not dumped:
            with open(f"{BASE}/sample_form4.xml", "w") as f:
                f.write(xml)
            dumped = True
        try:
            root = ET.fromstring(xml)
        except ET.ParseError:
            continue
        parsed += 1
        for el in root.iter():
            tag = el.tag
            all_tags[tag] += 1
            if "10b5" in tag.lower():
                tags_with_10b5[tag] += 1
            if el.text and "10b5" in el.text.lower():
                context_tags[tag] += 1
        for fn in root.iter("footnote"):
            if fn.text and re.search(r"10b5[\-\s]?1", fn.text, re.IGNORECASE):
                footnote_hits += 1

    print(f"Fetched/parsed {parsed}/{len(rows)} Form 4s")
    print(f"net={client.net_hits} cache={client.cache_hits} 404={client.not_found}")
    print("\n--- element names containing '10b5' ---")
    print(dict(tags_with_10b5) or "(none)")
    print("\n--- element names whose TEXT mentions '10b5' ---")
    print(dict(context_tags) or "(none)")
    print(f"\n--- footnotes matching 10b5-1 regex: {footnote_hits}")
    print("\n--- all element tags containing 'rule', '10b', or 'plan' ---")
    for tag, c in all_tags.items():
        if any(k in tag.lower() for k in ("rule", "10b", "plan")):
            print(f"  {tag}: {c}")
    print(f"\nDumped one raw sample -> {BASE}/sample_form4.xml")


if __name__ == "__main__":
    main()
