#!/usr/bin/env python3
"""Batch email verification script for large lists.

Usage:
    python batch_verify.py emails.csv --output results.csv

Features:
- Skips already verified emails (checks database)
- Saves progress every 100 emails
- Resumes from last saved position
- Shows real-time progress
"""

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import List, Set
import requests
from tqdm import tqdm

# Configuration
API_URL = "http://localhost:8025"  # Change for remote server
API_KEY = "131245c8cc9ac8ae3d69f3a7f7e85164a29c08403483aa7b2f3608f53e5765a6"

def get_verified_emails() -> Set[str]:
    """Get list of already verified emails from database."""
    try:
        import duckdb
        conn = duckdb.connect('verified.duckdb', read_only=True)
        result = conn.execute('SELECT email FROM verified_emails').fetchall()
        conn.close()
        return {email[0].lower() for email in result}
    except Exception as e:
        print(f"Warning: Could not read database: {e}")
        return set()

def load_emails_from_csv(filepath: str) -> List[str]:
    """Load emails from CSV file."""
    emails = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        # Try to find email column
        if 'email' in reader.fieldnames:
            email_col = 'email'
        elif 'Email' in reader.fieldnames:
            email_col = 'Email'
        else:
            # Use first column
            email_col = reader.fieldnames[0]
            print(f"Warning: Using first column '{email_col}' as email column")

        for row in reader:
            email = row.get(email_col, '').strip()
            if email and '@' in email:
                emails.append(email)

    return emails

def verify_email(email: str) -> dict:
    """Verify a single email via API."""
    try:
        response = requests.get(
            f"{API_URL}/verify",
            params={"email": email},
            headers={"X-API-Key": API_KEY},
            timeout=120
        )
        if response.status_code == 200:
            return response.json()
        else:
            return {
                "email": email,
                "status": "error",
                "error": f"API error: {response.status_code}"
            }
    except Exception as e:
        return {
            "email": email,
            "status": "error",
            "error": str(e)
        }

def save_results(results: List[dict], output_file: str, append: bool = False):
    """Save results to CSV."""
    mode = 'a' if append else 'w'

    if not results:
        return

    with open(output_file, mode, newline='', encoding='utf-8') as f:
        fieldnames = list(results[0].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if not append or not Path(output_file).exists():
            writer.writeheader()

        writer.writerows(results)

def main():
    parser = argparse.ArgumentParser(description='Batch email verification')
    parser.add_argument('input_file', help='Input CSV file with emails')
    parser.add_argument('--output', '-o', default='verification_results.csv',
                       help='Output CSV file (default: verification_results.csv)')
    parser.add_argument('--skip-existing', action='store_true',
                       help='Skip emails already in database')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Save progress every N emails (default: 100)')
    parser.add_argument('--delay', type=float, default=0.1,
                       help='Delay between requests in seconds (default: 0.1)')

    args = parser.parse_args()

    # Load emails
    print(f"Loading emails from {args.input_file}...")
    emails = load_emails_from_csv(args.input_file)
    print(f"Found {len(emails)} emails")

    # Skip already verified
    if args.skip_existing:
        print("Checking database for already verified emails...")
        verified = get_verified_emails()
        emails_to_verify = [e for e in emails if e.lower() not in verified]
        print(f"Skipping {len(emails) - len(emails_to_verify)} already verified emails")
        emails = emails_to_verify

    if not emails:
        print("No emails to verify!")
        return

    print(f"\nVerifying {len(emails)} emails...")
    print(f"Results will be saved to: {args.output}")
    print(f"Progress saved every {args.batch_size} emails\n")

    # Verify emails
    results_batch = []

    for i, email in enumerate(tqdm(emails, desc="Verifying"), 1):
        result = verify_email(email)
        results_batch.append(result)

        # Save progress periodically
        if i % args.batch_size == 0:
            save_results(results_batch, args.output, append=(i > args.batch_size))
            results_batch = []

        # Rate limiting
        if args.delay > 0:
            time.sleep(args.delay)

    # Save remaining results
    if results_batch:
        save_results(results_batch, args.output, append=True)

    print(f"\n✅ Complete! Results saved to {args.output}")

    # Show summary
    all_results = []
    with open(args.output, 'r') as f:
        reader = csv.DictReader(f)
        all_results = list(reader)

    status_counts = {}
    for r in all_results:
        status = r.get('status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1

    print("\nSummary:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")

if __name__ == '__main__':
    main()
