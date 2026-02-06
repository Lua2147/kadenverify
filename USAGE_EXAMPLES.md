# Usage Examples: Verifying Different Lists

## Overview

KadenVerify works with any email list in common formats:
- CSV files
- Text files (one email per line)
- DuckDB databases
- JSON files
- Direct API calls

---

## Example 1: CSV File (Contact List)

**Input file:** `contacts.csv`
```csv
first_name,last_name,email,company
John,Smith,john.smith@acme.com,Acme Corp
Jane,Doe,jane@example.com,Example Inc
Bob,Johnson,bob@catchall-company.com,CatchAll Co
```

**Verify all emails:**
```bash
python cli.py verify-file contacts.csv --format csv --output verified_contacts.csv
```

**Output file:** `verified_contacts.csv`
```csv
email,status,reachability,deliverable,catch_all,disposable,role,provider,confidence
john.smith@acme.com,valid,safe,true,false,false,false,generic,0.95
jane@example.com,valid,safe,true,false,false,false,gmail,0.98
bob@catchall-company.com,valid,risky,true,true,false,false,generic,0.85
```

---

## Example 2: Text File (Simple Email List)

**Input file:** `emails.txt`
```
user1@gmail.com
user2@yahoo.com
user3@company.com
test@disposable-domain.com
```

**Verify:**
```bash
python cli.py verify-file emails.txt
```

**Output:** (console)
```
✓ user1@gmail.com [safe]
✓ user2@yahoo.com [safe]
~ user3@company.com [risky] (catch-all)
✗ test@disposable-domain.com [invalid] (disposable)

Summary (4 emails):
  safe: 2 (50.0%)
  risky: 1 (25.0%)
  invalid: 1 (25.0%)
```

---

## Example 3: DuckDB Database (Large List)

**Database:** `contacts.duckdb` with table `people`
```sql
CREATE TABLE people (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    company VARCHAR,
    phone VARCHAR
);
```

**Verify all emails:**
```bash
python cli.py pipeline \
  --source-path contacts.duckdb \
  --table people \
  --email-column email \
  --concurrency 50
```

**Results stored in:** `verified.duckdb`

**Query results:**
```sql
-- Join original data with verification results
SELECT
    p.*,
    v.reachability,
    v.is_deliverable,
    v.is_catch_all,
    v.catchall_confidence
FROM people p
LEFT JOIN verified_emails v ON p.email = v.email
WHERE v.reachability = 'safe'
```

---

## Example 4: JSON File (API Export)

**Input file:** `contacts.json`
```json
[
  {
    "id": 1,
    "name": "John Smith",
    "email": "john@company.com"
  },
  {
    "id": 2,
    "name": "Jane Doe",
    "email": "jane@example.com"
  }
]
```

**Convert to CSV, then verify:**
```bash
# Extract emails to text file
jq -r '.[].email' contacts.json > emails.txt

# Verify
python cli.py verify-file emails.txt --format json --output verified.json
```

---

## Example 5: CRM Export (Salesforce, HubSpot, etc.)

**Typical CRM CSV:**
```csv
Contact ID,First Name,Last Name,Email,Company,Title,Phone,Status
001,John,Smith,john.smith@acme.com,Acme Corp,CEO,555-1234,Active
002,Jane,Doe,jane@example.com,Example Inc,CTO,555-5678,Active
```

**Verify with catch-all enhancement:**
```bash
# If you have Apollo database for cross-reference
export APOLLO_DB_PATH=/path/to/apollo.duckdb

python cli.py verify-file crm_export.csv \
  --format csv \
  --output crm_verified.csv \
  --concurrency 30
```

**Output includes catch-all confidence:**
```csv
email,status,catchall_confidence,catchall_likely_real,catchall_reasons
john.smith@acme.com,valid,0.92,true,"apollo_match,name_pattern_match_0.95"
jane@example.com,valid,null,null,null
```

---

## Example 6: Marketing List (Email Blast)

**Goal:** Clean list before sending campaign

**Input:** 500K emails from various sources

**Step 1: Verify**
```bash
python cli.py verify-file marketing_list.csv \
  --format csv \
  --output clean_list.csv \
  --concurrency 50
```

**Step 2: Filter by quality**
```python
import pandas as pd

# Load verified list
df = pd.read_csv('clean_list.csv')

# Filter by criteria
high_quality = df[
    (df['status'] == 'valid') &
    (df['reachability'] == 'safe') &
    (df['is_role'] == False) &
    (df['is_disposable'] == False)
]

medium_quality = df[
    (df['status'] == 'valid') &
    (df['reachability'] == 'risky') &
    (df['catchall_confidence'] >= 0.75)
]

# Save segments
high_quality.to_csv('send_immediately.csv', index=False)
medium_quality.to_csv('send_cautiously.csv', index=False)
```

**Step 3: Track bounces for future**
```bash
# After campaign, feed bounce data back
python cli.py update-bounces --bounce-file campaign_bounces.csv
```

---

## Example 7: Real-Time Signup Validation

**Use case:** Validate email during user registration

**API integration:**
```javascript
// Frontend form validation
async function validateEmail(email) {
  const response = await fetch(
    `https://verify.yourdomain.com/verify?email=${encodeURIComponent(email)}`,
    {
      headers: {
        'X-API-Key': 'your-api-key'
      }
    }
  );

  const result = await response.json();

  if (result.status === 'valid' && result.reachability === 'safe') {
    return { valid: true, message: 'Email verified!' };
  } else if (result.status === 'valid' && result.is_catch_all) {
    return {
      valid: true,
      warning: 'Email may not receive messages (catch-all domain)'
    };
  } else if (result.is_disposable) {
    return {
      valid: false,
      error: 'Disposable email addresses not allowed'
    };
  } else {
    return {
      valid: false,
      error: 'Email address appears invalid'
    };
  }
}
```

---

## Example 8: Multi-Source List Consolidation

**Scenario:** Combine emails from multiple sources

**Sources:**
- CRM export (50K)
- Website signups (20K)
- Event attendees (10K)
- Partner list (5K)

**Consolidate and verify:**
```bash
# Step 1: Merge all sources
cat crm_export.csv website_signups.csv events.csv partners.csv > all_contacts.csv

# Step 2: Deduplicate
sort -u all_contacts.csv > unique_contacts.csv

# Step 3: Verify
python cli.py verify-file unique_contacts.csv \
  --format csv \
  --output verified_all.csv \
  --concurrency 50

# Step 4: Import to database
python -c "
import pandas as pd
import duckdb

df = pd.read_csv('verified_all.csv')
conn = duckdb.connect('master_list.duckdb')
df.to_sql('contacts', conn, if_exists='replace', index=False)
"
```

---

## Example 9: Incremental Verification (New Contacts Only)

**Use case:** Daily verification of new contacts

**Setup cron job:**
```bash
#!/bin/bash
# verify_new_contacts.sh

# Export new contacts from CRM (last 24 hours)
python export_new_contacts.py > new_contacts_$(date +%Y%m%d).csv

# Verify only new contacts
python cli.py verify-file new_contacts_$(date +%Y%m%d).csv \
  --format csv \
  --output verified_$(date +%Y%m%d).csv

# Import back to CRM
python import_verification_results.py verified_$(date +%Y%m%d).csv
```

**Crontab entry:**
```
0 2 * * * /path/to/verify_new_contacts.sh
```

---

## Example 10: Apollo Database Integration

**Use case:** Validate catch-all emails using Apollo data

**Setup:**
```bash
# Point to your Apollo database
export APOLLO_DB_PATH=/path/to/apollo.duckdb

# Or in config.json:
{
  "catchall_validation": {
    "apollo_db_path": "/path/to/apollo.duckdb"
  }
}
```

**Verify with Apollo cross-reference:**
```bash
python cli.py verify-file catchall_emails.txt --format csv --output validated.csv
```

**Results:**
```csv
email,status,catchall_confidence,apollo_match
john@catchall.com,valid,0.90,true
jane@catchall.com,valid,0.75,false
random@catchall.com,risky,0.20,false
```

**Apollo match rate:** ~30% of emails will match your Apollo database

---

## Performance Tips

### 1. **Use High Concurrency for Large Lists**
```bash
# For 100K+ emails, use 50+ concurrency
python cli.py verify-file large_list.csv --concurrency 50
```

### 2. **Use DuckDB for Very Large Lists**
```bash
# For 1M+ emails, use DuckDB pipeline
python cli.py pipeline --source-path contacts.duckdb --concurrency 100
```

### 3. **Batch Processing for Multiple Lists**
```bash
# Process multiple lists overnight
for file in lists/*.csv; do
  python cli.py verify-file "$file" --output "verified_$(basename $file)"
done
```

### 4. **Use API for Real-Time Validation**
```bash
# For user signups, forms, etc.
curl 'http://localhost:8025/verify?email=user@example.com'
```

### 5. **Cache Warmup for Repeated Verification**
```bash
# First run: 2-5s per email (SMTP)
python cli.py pipeline --source qualified --concurrency 50

# Subsequent runs: <50ms per email (cached)
curl 'http://localhost:8025/verify?email=any@qualified.com'
```

---

## Common Workflows

### Weekly List Cleaning
```bash
# 1. Export from CRM
# 2. Verify
python cli.py verify-file weekly_export.csv --concurrency 30 --output verified.csv
# 3. Import back to CRM
# 4. Archive old verifications (>30 days)
```

### Pre-Campaign Verification
```bash
# 1. Export campaign list
# 2. Verify + filter
python cli.py verify-file campaign.csv --format csv | \
  grep -E "safe|valid" > clean_campaign.csv
# 3. Load to email sender
# 4. Track bounces for future
```

### Continuous Verification
```bash
# 1. Set up API
uvicorn server:app --host 0.0.0.0 --port 8025

# 2. Integrate with application
# All email inputs validated in real-time

# 3. Build cache over time
# 99% cache hit rate after initial verification
```

---

## Need Help?

See README.md for full documentation, or:
- CLI help: `python cli.py --help`
- API docs: http://localhost:8025/docs
- Examples: This file
