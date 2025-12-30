#!/usr/bin/env python3
"""
Fetch patients from DemoMed API, compute risk scores, and optionally submit results.

Usage:
    python assessment.py [--submit] [--limit N]

By default the script does a dry-run (no POST). Use `--submit` to send one submission.
Set `ASSESSMENT_API_KEY` env var to override the embedded key.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = os.environ.get("ASSESSMENT_BASE_URL", "https://assessment.ksensetech.com")
DEFAULT_API_KEY = os.environ.get(
    "ASSESSMENT_API_KEY",
    "ak_c7457de03ef7ea9f11c08daf379bd8dd1e78df659c3baee5",
)

logger = logging.getLogger("assessment")


def get_session(retries: int = 5, backoff: float = 1.0) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def fetch_patients(session: requests.Session, api_key: str, limit: int = 20) -> List[Dict[str, Any]]:
    patients: List[Dict[str, Any]] = []
    page = 1
    headers = {"x-api-key": api_key}

    while True:
        params = {"page": page, "limit": limit}
        url = f"{BASE_URL}/api/patients"
        logger.debug("GET %s %s", url, params)
        resp = session.get(url, headers=headers, params=params, timeout=10)

        if resp.status_code in (429, 500, 502, 503):
            # Let retries/backoff handle repeated transient failures. If reached here,
            # do an exponential sleep then continue.
            sleep = 1 + page
            logger.warning("Transient error %s; sleeping %ss", resp.status_code, sleep)
            time.sleep(sleep)
            continue

        try:
            payload = resp.json()
        except Exception:
            logger.error("Invalid JSON on page %s: %s", page, resp.text[:200])
            break

        data = payload.get("data") if isinstance(payload, dict) else None
        if not data:
            logger.debug("No data on page %s, stopping", page)
            break

        patients.extend(data)

        pagination = payload.get("pagination", {})
        has_next = pagination.get("hasNext")
        total_pages = pagination.get("totalPages")

        if has_next is False:
            break
        if total_pages and page >= int(total_pages):
            break

        page += 1

    return patients


def extract_numbers(s: Optional[str]) -> List[int]:
    if not s or not isinstance(s, str):
        return []
    nums = re.findall(r"\d{1,3}", s)
    return [int(n) for n in nums]


def parse_bp(bp_raw: Any) -> Tuple[Optional[int], Optional[int]]:
    if bp_raw is None:
        return None, None
    if isinstance(bp_raw, (int, float)):
        return None, None
    if not isinstance(bp_raw, str):
        return None, None
    nums = extract_numbers(bp_raw)
    if len(nums) >= 2:
        return nums[0], nums[1]
    return None, None


def bp_score(systolic: Optional[int], diastolic: Optional[int]) -> int:
    if systolic is None or diastolic is None:
        return 0
    try:
        s = int(systolic)
        d = int(diastolic)
    except Exception:
        return 0

    # Determine risk stage; if systolic and diastolic fall into different categories,
    # use the higher risk stage.
    scores = []
    # Normal
    if s < 120 and d < 80:
        scores.append(1)
    # Elevated
    if 120 <= s <= 129 and d < 80:
        scores.append(2)
    # Stage 1
    if (130 <= s <= 139) or (80 <= d <= 89):
        scores.append(3)
    # Stage 2
    if s >= 140 or d >= 90:
        scores.append(4)

    return max(scores) if scores else 0


def parse_temp(temp_raw: Any) -> Optional[float]:
    if temp_raw is None:
        return None
    if isinstance(temp_raw, (int, float)):
        return float(temp_raw)
    if not isinstance(temp_raw, str):
        return None
    # Remove non-numeric except dot and minus
    m = re.search(r"-?\d+\.?\d*", temp_raw)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def temp_score(temp_f: Optional[float]) -> int:
    if temp_f is None:
        return 0
    # Normal (<=99.5): 0 points
    if temp_f <= 99.5:
        return 0
    # Low Fever (99.6 - 100.9): 1 point
    if 99.6 <= temp_f <= 100.9:
        return 1
    # High Fever (>=101.0): 2 points
    if temp_f >= 101.0:
        return 2
    return 0


def parse_age(age_raw: Any) -> Optional[int]:
    if age_raw is None:
        return None
    if isinstance(age_raw, (int, float)):
        return int(age_raw)
    if not isinstance(age_raw, str):
        return None
    m = re.search(r"\d{1,3}", age_raw)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None


def age_score(age: Optional[int]) -> int:
    if age is None:
        return 0
    if age > 65:
        return 2
    # Under 40 and 40-65 both score 1 according to spec
    return 1


def analyze_patients(patients: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    high_risk: List[str] = []
    fever: List[str] = []
    data_issues: List[str] = []

    for p in patients:
        pid = p.get("patient_id") or p.get("id")
        if not pid:
            # skip records without an identifier
            continue

        bp_raw = p.get("blood_pressure")
        systolic, diastolic = parse_bp(bp_raw)
        bp_invalid = systolic is None or diastolic is None

        temp_raw = p.get("temperature")
        temp = parse_temp(temp_raw)
        temp_invalid = temp is None

        age_raw = p.get("age")
        age = parse_age(age_raw)
        age_invalid = age is None

        if bp_invalid or temp_invalid or age_invalid:
            data_issues.append(pid)

        bscore = bp_score(systolic, diastolic)
        tscore = temp_score(temp)
        ascore = age_score(age)

        total = bscore + tscore + ascore

        if total >= 4:
            high_risk.append(pid)
        if temp is not None and temp >= 99.6:
            fever.append(pid)

    # Ensure unique and stable ordering
    return {
        "high_risk_patients": sorted(set(high_risk)),
        "fever_patients": sorted(set(fever)),
        "data_quality_issues": sorted(set(data_issues)),
    }


def submit_results(session: requests.Session, api_key: str, payload: Dict[str, List[str]]) -> Dict[str, Any]:
    url = f"{BASE_URL}/api/submit-assessment"
    headers = {"x-api-key": api_key, "Content-Type": "application/json"}
    resp = session.post(url, headers=headers, json=payload, timeout=10)
    try:
        return resp.json()
    except Exception:
        return {"status_code": resp.status_code, "text": resp.text}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--submit", action="store_true", help="POST results to the assessment API")
    parser.add_argument("--limit", type=int, default=20, help="Page size for GET /patients (max 20)")
    parser.add_argument("--api-key", default=DEFAULT_API_KEY, help="API key override")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format="%(levelname)s: %(message)s")

    session = get_session()

    patients = fetch_patients(session, args.api_key, limit=args.limit)
    logger.info("Fetched %d patient records", len(patients))

    results = analyze_patients(patients)

    print(json.dumps(results, indent=2))

    if args.submit:
        logger.info("Submitting results to %s", BASE_URL)
        resp = submit_results(session, args.api_key, results)
        print(json.dumps(resp, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
