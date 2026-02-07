#!/usr/bin/env python3
"""Quick KadenVerify API Usage Example"""

import requests

# Your API configuration
API_KEY = "131245c8cc9ac8ae3d69f3a7f7e85164a29c08403483aa7b2f3608f53e5765a6"
API_URL = "http://localhost:8000"  # Change to production: http://198.23.249.137:8025

def verify_email(email: str) -> dict:
    """Verify a single email address."""
    response = requests.get(
        f"{API_URL}/verify",
        params={"email": email},
        headers={"X-API-Key": API_KEY}
    )
    response.raise_for_status()
    return response.json()

def verify_batch(emails: list) -> list:
    """Verify multiple email addresses."""
    response = requests.post(
        f"{API_URL}/verify/batch",
        json={"emails": emails},
        headers={"X-API-Key": API_KEY}
    )
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    # Single email example
    print("=== Single Email Verification ===")
    result = verify_email("test@gmail.com")
    print(f"Email: {result['email']}")
    print(f"Status: {result['status']}")
    print(f"Deliverable: {result.get('is_deliverable', 'N/A')}")
    print(f"Tier: {result.get('_kadenverify_tier', 'N/A')}")
    print(f"Reason: {result.get('_kadenverify_reason', 'N/A')}")
    print()

    # Batch example
    print("=== Batch Verification ===")
    emails = [
        "louis@kadenwood.com",
        "test@gmail.com",
        "invalid@fake-domain.com"
    ]
    results = verify_batch(emails)
    print(f"Verified {len(results)} emails:")
    for r in results:
        print(f"  {r['email']:30} → {r['status']}")
