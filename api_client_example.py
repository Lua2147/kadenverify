"""KadenVerify API Client Example

Usage:
    python api_client_example.py single test@example.com
    python api_client_example.py batch emails.txt
    python api_client_example.py health
"""

import sys
import requests
from typing import List, Dict

# Configuration
API_BASE_URL = "http://localhost:8000"  # Change to production URL as needed
API_KEY = "your-secret-key-here"  # Replace with actual key


def verify_single(email: str) -> Dict:
    """Verify a single email address."""
    response = requests.get(
        f"{API_BASE_URL}/verify",
        params={"email": email},
        headers={"X-API-Key": API_KEY}
    )
    response.raise_for_status()
    return response.json()


def verify_batch(emails: List[str]) -> List[Dict]:
    """Verify multiple email addresses."""
    response = requests.post(
        f"{API_BASE_URL}/verify/batch",
        json={"emails": emails},
        headers={"X-API-Key": API_KEY}
    )
    response.raise_for_status()
    return response.json()


def health_check() -> Dict:
    """Check API health status."""
    response = requests.get(f"{API_BASE_URL}/health")
    response.raise_for_status()
    return response.json()


def get_stats() -> Dict:
    """Get verification statistics."""
    response = requests.get(
        f"{API_BASE_URL}/stats",
        headers={"X-API-Key": API_KEY}
    )
    response.raise_for_status()
    return response.json()


def check_credits() -> Dict:
    """Check available credits (OmniVerifier compatibility)."""
    response = requests.get(
        f"{API_BASE_URL}/v1/validate/credits",
        headers={"x-api-key": API_KEY}
    )
    response.raise_for_status()
    return response.json()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    try:
        if command == "single":
            if len(sys.argv) < 3:
                print("Usage: python api_client_example.py single <email>")
                sys.exit(1)

            email = sys.argv[2]
            result = verify_single(email)

            print(f"\n✓ Verification Result for {email}")
            print(f"  Status:       {result['status']}")
            print(f"  Deliverable:  {result['is_deliverable']}")
            print(f"  Catch-all:    {result['is_catch_all']}")
            print(f"  Disposable:   {result['is_disposable']}")
            print(f"  Role:         {result['is_role']}")
            print(f"  Free:         {result['is_free']}")
            print(f"  Provider:     {result.get('provider', 'unknown')}")
            print(f"  MX Host:      {result.get('mx_host', 'N/A')}")

            if '_kadenverify_tier' in result:
                print(f"  Tier:         {result['_kadenverify_tier']}")
                print(f"  Reason:       {result['_kadenverify_reason']}")

        elif command == "batch":
            if len(sys.argv) < 3:
                print("Usage: python api_client_example.py batch <emails_file.txt>")
                sys.exit(1)

            filepath = sys.argv[2]
            with open(filepath) as f:
                emails = [line.strip() for line in f if line.strip()]

            print(f"\n⏳ Verifying {len(emails)} emails...")
            results = verify_batch(emails)

            valid_count = sum(1 for r in results if r['status'] == 'valid')
            invalid_count = sum(1 for r in results if r['status'] == 'invalid')
            catchall_count = sum(1 for r in results if r['status'] == 'catch_all')
            unknown_count = sum(1 for r in results if r['status'] == 'unknown')

            print(f"\n✓ Batch Verification Complete")
            print(f"  Total:     {len(results)}")
            print(f"  Valid:     {valid_count}")
            print(f"  Invalid:   {invalid_count}")
            print(f"  Catch-all: {catchall_count}")
            print(f"  Unknown:   {unknown_count}")

            # Save results
            output_file = filepath.replace('.txt', '_results.txt')
            with open(output_file, 'w') as f:
                for result in results:
                    f.write(f"{result['email']},{result['status']}\n")
            print(f"\n  Results saved to {output_file}")

        elif command == "health":
            result = health_check()
            print(f"\n✓ Health Check")
            print(f"  Status:  {result['status']}")
            print(f"  Service: {result['service']}")
            print(f"  Version: {result['version']}")

        elif command == "stats":
            result = get_stats()
            print(f"\n✓ Verification Statistics")
            for key, value in result.items():
                print(f"  {key.capitalize():12} {value}")

        elif command == "credits":
            result = check_credits()
            print(f"\n✓ Available Credits")
            print(f"  Credits:   {result['credits']:,}")
            print(f"  Remaining: {result['remaining']:,}")

        else:
            print(f"Unknown command: {command}")
            print(__doc__)
            sys.exit(1)

    except requests.exceptions.HTTPError as e:
        print(f"\n✗ API Error: {e}")
        if e.response is not None:
            print(f"  Response: {e.response.text}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
