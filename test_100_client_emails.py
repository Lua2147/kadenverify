#!/usr/bin/env python3
"""Test KadenVerify with 100 real client emails from Bitcoin Hotel."""

import requests
import csv
import time
from collections import Counter

API_URL = "http://149.28.37.34:8025"
API_KEY = "kadenwood_verify_2026"

def main() -> None:
    # Read client emails (kept as a local harness; guarded so pytest collection doesn't execute it).
    with open("/Users/louis/Mundi Princeps/data/exports/bitcoin_hotel_emails.csv") as f:
        reader = csv.DictReader(f)
        all_emails = [row["email"].strip().lower() for row in reader if row["email"].strip()]

    # Test first 100 emails
    test_emails = all_emails[:100]

    print("=" * 80)
    print("TESTING KADENVERIFY WITH 100 REAL CLIENT EMAILS")
    print("=" * 80)
    print("\nDataset: Bitcoin Hotel investor emails")
    print(f"Total in file: {len(all_emails)} emails")
    print(f"Testing: {len(test_emails)} emails")
    print()

    results = []
    tier_distribution = Counter()
    status_distribution = Counter()
    enrichment_count = 0
    total_time = 0
    total_cost = 0.0

    for i, email in enumerate(test_emails, 1):
        print(f"[{i:3}/{len(test_emails)}] {email:45}", end=" ", flush=True)

        start = time.time()
        try:
            response = requests.get(
                f"{API_URL}/verify",
                params={"email": email},
                headers={"X-API-Key": API_KEY},
                timeout=30,
            )
            duration = time.time() - start
            total_time += duration

            if response.status_code == 200:
                result = response.json()
                status = result.get("status", "unknown")
                tier = result.get("_kadenverify_tier", "?")
                reason = result.get("_kadenverify_reason", "N/A")

                tier_distribution[f"Tier {tier}"] += 1
                status_distribution[status] += 1

                # Estimate cost based on tier
                if "tier5" in reason or "tier6" in reason:
                    if "exa" in reason:
                        total_cost += 0.0005
                    if "apollo" in reason:
                        total_cost += 0.10
                    enrichment_count += 1
                    icon = "🎉"
                elif status == "valid":
                    icon = "✅"
                elif status == "invalid":
                    icon = "❌"
                elif status == "catch_all":
                    icon = "⚠️"
                else:
                    icon = "❓"

                print(f"{icon} {status:10} T{tier} {duration:4.1f}s")

                results.append(
                    {
                        "email": email,
                        "status": status,
                        "tier": tier,
                        "reason": reason,
                        "duration": duration,
                    }
                )
            else:
                print(f"❌ HTTP {response.status_code}")
                tier_distribution["Error"] += 1

        except Exception as e:
            print(f"❌ Error: {e}")
            tier_distribution["Error"] += 1

    print()
    print("=" * 80)
    print("RESULTS SUMMARY")
    print("=" * 80)
    print()

    print("📊 Status Distribution:")
    for status, count in sorted(status_distribution.items()):
        pct = (count / len(test_emails)) * 100
        bar = "█" * int(pct / 2)
        print(f"   {status.upper():15} {count:3} ({pct:5.1f}%) {bar}")

    print()
    print("🏷️  Tier Distribution:")
    for tier, count in sorted(tier_distribution.items()):
        pct = (count / len(test_emails)) * 100
        bar = "█" * int(pct / 2)
        print(f"   {tier:15} {count:3} ({pct:5.1f}%) {bar}")

    print()
    print("🎉 Enrichment Stats:")
    print(f"   Emails enriched:     {enrichment_count}")
    print(f"   Enrichment rate:     {(enrichment_count / len(test_emails)) * 100:.1f}%")
    print(f"   Actual cost:         ${total_cost:.2f}")

    print()
    print("⏱️  Performance:")
    print(f"   Total time:          {total_time:.1f}s ({total_time/60:.1f} min)")
    print(f"   Avg per email:       {total_time / len(test_emails):.2f}s")
    print(f"   Throughput:          {len(test_emails) / total_time:.2f} emails/sec")

    print()
    print("💰 Cost Analysis:")
    print(f"   Cost for 100 emails: ${total_cost:.2f}")
    print(f"   Cost per email:      ${total_cost / len(test_emails):.4f}")
    print(f"   Projected 1000:      ${total_cost * 10:.2f}")
    print(f"   Projected 740 (all): ${total_cost * 7.4:.2f}")

    print()
    print("📈 Comparison:")
    print(
        f"   vs Pure Apollo:      ${len(test_emails) * 0.10:.2f} "
        f"(saved ${len(test_emails) * 0.10 - total_cost:.2f})"
    )
    print(
        f"   vs OmniVerifier:     ${len(test_emails) * 0.046:.2f} "
        f"(saved ${len(test_emails) * 0.046 - total_cost:.2f})"
    )

    print()
    print("=" * 80)
    print("✅ Test complete!")


if __name__ == "__main__":
    main()
