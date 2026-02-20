"""KadenVerify Web Dashboard - Streamlit UI for email verification.

Features:
- File upload (CSV, TXT)
- Real-time verification with progress
- Results table with filters
- Download verified results
- Statistics dashboard
"""

import streamlit as st
import pandas as pd
import requests
import io
import time
from datetime import datetime
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
API_URL = "http://localhost:8025"  # Local API with port 25 now unblocked!
API_KEY = st.secrets.get("KADENVERIFY_API_KEY", "kadenwood_verify_2026")

# Create a session for connection pooling
SESSION = requests.Session()
SESSION.headers.update({"X-API-Key": API_KEY})

# Page config
st.set_page_config(
    page_title="KadenVerify - Email Verification",
    page_icon="✉️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }
    .stat-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stat-number {
        font-size: 2rem;
        font-weight: 700;
    }
    .stat-label {
        font-size: 0.875rem;
        opacity: 0.9;
    }
</style>
""", unsafe_allow_html=True)


def verify_email(email: str) -> Dict:
    """Verify a single email via API."""
    try:
        response = SESSION.get(
            f"{API_URL}/verify",
            params={"email": email},
            timeout=120  # Increased for slow SMTP servers
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


def verify_batch(emails: List[str], progress_callback=None, max_workers=10) -> List[Dict]:
    """Verify multiple emails with concurrent processing and progress tracking."""
    results = []
    total = len(emails)
    completed = 0

    # Use ThreadPoolExecutor for concurrent requests
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_email = {executor.submit(verify_email, email): email for email in emails}

        # Process completed futures as they finish
        for future in as_completed(future_to_email):
            result = future.result()
            results.append(result)
            completed += 1

            if progress_callback:
                progress_callback(completed, total)

    # Sort results to match input order
    email_to_result = {r['email']: r for r in results}
    ordered_results = [email_to_result.get(email, {"email": email, "status": "error", "error": "not processed"})
                       for email in emails]

    # Option 3: Retry unknowns with different strategy
    unknowns = [r for r in ordered_results if r.get('status') == 'unknown']
    if unknowns and progress_callback:
        progress_callback(completed, total, retry_phase=True, unknowns_count=len(unknowns))

    if unknowns:
        time.sleep(2)  # Brief delay before retry

        # Retry with longer timeout and lower concurrency
        retry_results = []
        for i, unknown in enumerate(unknowns):
            try:
                # Longer timeout for retry
                response = SESSION.get(
                    f"{API_URL}/verify",
                    params={"email": unknown['email'], "force_full_check": "true"},
                    timeout=300  # 5 minute timeout for difficult domains
                )
                if response.status_code == 200:
                    retry_result = response.json()
                    retry_results.append(retry_result)
                else:
                    retry_results.append(unknown)

                # Progress update for retries
                if progress_callback and i % 5 == 0:
                    progress_callback(completed, total, retry_phase=True,
                                    retry_progress=f"{i+1}/{len(unknowns)}")

                time.sleep(1)  # Delay between retries to avoid rate limiting
            except:
                retry_results.append(unknown)

        # Merge retry results back
        retry_email_to_result = {r['email']: r for r in retry_results}
        ordered_results = [retry_email_to_result.get(r['email'], r)
                          for r in ordered_results]

    # Option 1: Reclassify remaining unknowns as risky
    for result in ordered_results:
        if result.get('status') == 'unknown':
            result['status'] = 'risky'
            result['reachability'] = 'risky'
            result['is_deliverable'] = False
            if 'error' not in result:
                result['risk_reason'] = 'Unable to verify - treat as risky'

    return ordered_results


def parse_uploaded_file(uploaded_file) -> List[str]:
    """Parse uploaded file and extract emails."""
    emails = []

    if uploaded_file.name.endswith('.csv'):
        # Try to find email column
        df = pd.read_csv(uploaded_file)

        # Look for email column
        email_cols = [col for col in df.columns if 'email' in col.lower()]
        if email_cols:
            emails = df[email_cols[0]].dropna().tolist()
        elif len(df.columns) == 1:
            emails = df.iloc[:, 0].dropna().tolist()
        else:
            st.error("Could not find email column. Please ensure CSV has an 'email' column.")
            return []
    else:
        # Text file - one email per line
        content = uploaded_file.getvalue().decode('utf-8')
        emails = [line.strip() for line in content.split('\n')
                 if line.strip() and '@' in line and not line.startswith('#')]

    return emails


def calculate_tier_stats(results: List[Dict]) -> Dict:
    """Calculate tier distribution and cost statistics."""
    tier_counts = {}
    enrichment_count = 0
    total_cost = 0.0

    for result in results:
        tier = result.get('_kadenverify_tier', 'unknown')
        reason = result.get('_kadenverify_reason', '')

        # Count by tier
        tier_key = f"Tier {tier}"
        tier_counts[tier_key] = tier_counts.get(tier_key, 0) + 1

        # Count enrichment and calculate cost
        if 'tier5' in reason or 'tier6' in reason:
            enrichment_count += 1
            if 'exa' in reason:
                total_cost += 0.0005
            if 'apollo' in reason:
                total_cost += 0.10

    return {
        'tier_counts': tier_counts,
        'enrichment_count': enrichment_count,
        'total_cost': total_cost,
        'cost_per_email': total_cost / len(results) if results else 0,
        'enrichment_rate': enrichment_count / len(results) if results else 0
    }


def results_to_dataframe(results: List[Dict]) -> pd.DataFrame:
    """Convert results to pandas DataFrame."""
    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)

    # Reorder columns for better display
    priority_cols = ['email', 'status', '_kadenverify_tier', '_kadenverify_reason',
                     'reachability', 'is_deliverable', 'is_catchall',
                     'catchall_confidence', 'is_disposable', 'is_role',
                     'provider', 'mx_host']

    cols = [col for col in priority_cols if col in df.columns]
    cols += [col for col in df.columns if col not in cols]

    return df[cols]


def main():
    # Header
    st.markdown('<h1 class="main-header">✉️ KadenVerify</h1>', unsafe_allow_html=True)
    st.markdown("**Self-hosted email verification** - Zero cost, 95%+ accuracy")

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Settings")

        # API connection test
        st.subheader("API Status")
        try:
            response = requests.get(f"{API_URL}/health", timeout=5)
            if response.status_code == 200:
                st.success("✅ API Connected")
            else:
                st.error("❌ API Error")
        except:
            st.error("❌ API Offline")

        st.divider()

        # Stats (if available)
        st.subheader("📊 Statistics")
        try:
            response = requests.get(
                f"{API_URL}/stats",
                headers={"X-API-Key": API_KEY},
                timeout=5
            )
            if response.status_code == 200:
                stats = response.json()
                st.metric("Total Verified", f"{stats.get('total', 0):,}")
                st.metric("Cache Hit Rate", f"{stats.get('cache_hit_rate', 0):.1%}")
        except:
            st.info("Stats unavailable")

        st.divider()

        # Help
        st.subheader("ℹ️ About")
        st.markdown("""
        **KadenVerify** uses a 6-tier pipeline:
        - **Tier 1:** Cache (<50ms) - FREE
        - **Tier 2:** Fast validation (DNS) - FREE
        - **Tier 3:** SMTP verification - FREE
        - **Tier 4:** Pattern matching - FREE
        - **Tier 5:** Enrichment (Exa/Apollo) - $0.0005-$0.10
        - **Tier 6:** SMTP re-verification - FREE

        **Status meanings:**
        - ✅ **valid** - Safe to send
        - ⚠️ **risky** - Send with caution
        - ❌ **invalid** - Will bounce

        **Cost:** ~$7 per 1000 emails
        (93% cheaper than pure Apollo!)
        """)

    # Main content
    tab1, tab2, tab3, tab4 = st.tabs(["📁 Upload & Verify", "⚡ Single Email", "📊 Batch Results", "💾 Database Results"])

    # Tab 1: File Upload
    with tab1:
        st.header("Upload Email List")

        uploaded_file = st.file_uploader(
            "Choose a file",
            type=['csv', 'txt'],
            help="CSV with 'email' column or TXT with one email per line"
        )

        if uploaded_file:
            with st.spinner("Parsing file..."):
                emails = parse_uploaded_file(uploaded_file)

            if emails:
                st.success(f"✅ Found {len(emails):,} emails")

                # Preview
                with st.expander("📋 Preview emails", expanded=False):
                    st.write(emails[:10])
                    if len(emails) > 10:
                        st.info(f"Showing first 10 of {len(emails)} emails")

                # Verify button
                verify_button = st.button("🚀 Verify All", type="primary", use_container_width=True)

                if verify_button:
                    emails_to_verify = emails

                    st.subheader("⚡ Verification in Progress")
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    def update_progress(current, total, retry_phase=False, unknowns_count=0, retry_progress=""):
                        if retry_phase:
                            if retry_progress:
                                status_text.text(f"🔄 Retrying unknowns with extended timeout... {retry_progress}")
                            else:
                                status_text.text(f"🔄 Found {unknowns_count} unknowns - retrying with longer timeout...")
                        else:
                            progress = current / total
                            progress_bar.progress(progress)
                            status_text.text(f"Verified {current}/{total} emails ({progress:.0%})")

                    # Verify
                    start_time = time.time()
                    results = verify_batch(emails_to_verify, update_progress)
                    duration = time.time() - start_time

                    # Store results in session state
                    st.session_state['results'] = results
                    st.session_state['verification_time'] = duration

                    st.success(f"✅ Verification complete! ({duration:.1f}s)")

                    # Show summary
                    st.subheader("📊 Summary")
                    df = results_to_dataframe(results)
                    tier_stats = calculate_tier_stats(results)

                    # Status distribution
                    col1, col2, col3, col4 = st.columns(4)

                    safe_count = len(df[df['status'] == 'valid'])
                    risky_count = len(df[df['status'].isin(['catch_all', 'risky', 'unknown'])])
                    invalid_count = len(df[df['status'] == 'invalid'])
                    error_count = len(df[df['status'] == 'error'])

                    with col1:
                        st.markdown(f"""
                        <div class="stat-box" style="background: linear-gradient(135deg, #34d399 0%, #10b981 100%);">
                            <div class="stat-number">{safe_count}</div>
                            <div class="stat-label">✅ Safe</div>
                        </div>
                        """, unsafe_allow_html=True)

                    with col2:
                        st.markdown(f"""
                        <div class="stat-box" style="background: linear-gradient(135deg, #fbbf24 0%, #f59e0b 100%);">
                            <div class="stat-number">{risky_count}</div>
                            <div class="stat-label">⚠️ Risky</div>
                        </div>
                        """, unsafe_allow_html=True)

                    with col3:
                        st.markdown(f"""
                        <div class="stat-box" style="background: linear-gradient(135deg, #f87171 0%, #dc2626 100%);">
                            <div class="stat-number">{invalid_count}</div>
                            <div class="stat-label">❌ Invalid</div>
                        </div>
                        """, unsafe_allow_html=True)

                    with col4:
                        st.markdown(f"""
                        <div class="stat-box" style="background: linear-gradient(135deg, #94a3b8 0%, #64748b 100%);">
                            <div class="stat-number">{error_count}</div>
                            <div class="stat-label">⚠️ Errors</div>
                        </div>
                        """, unsafe_allow_html=True)

                    st.divider()

                    # Tier distribution and cost
                    st.subheader("💰 Cost & Efficiency")
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.metric("Total Cost", f"${tier_stats['total_cost']:.2f}")
                        st.caption(f"${tier_stats['cost_per_email']:.4f} per email")

                    with col2:
                        st.metric("Enriched", f"{tier_stats['enrichment_count']}")
                        st.caption(f"{tier_stats['enrichment_rate']:.1%} needed Tier 5")

                    with col3:
                        projected_1000 = tier_stats['cost_per_email'] * 1000
                        st.metric("Projected 1000", f"${projected_1000:.2f}")
                        st.caption(f"vs ${100:.2f} Apollo (${100 - projected_1000:.2f} saved)")

                    # Tier distribution chart
                    with st.expander("📊 Tier Distribution", expanded=False):
                        tier_df = pd.DataFrame([
                            {"Tier": k, "Count": v, "Percentage": f"{(v/len(results)*100):.1f}%"}
                            for k, v in sorted(tier_stats['tier_counts'].items())
                        ])
                        st.dataframe(tier_df, use_container_width=True, hide_index=True)

                        # Show what each tier means
                        st.caption("""
                        **Tier 1:** Cache • **Tier 2:** Fast DNS • **Tier 3:** SMTP
                        • **Tier 4:** Pattern • **Tier 5:** Enrichment (paid) • **Tier 6:** SMTP Loop
                        """)

                    st.divider()

                    # Show results table
                    st.subheader("📋 Results")

                    # Add enrichment indicator column
                    if '_kadenverify_reason' in df.columns:
                        df['enriched'] = df['_kadenverify_reason'].apply(
                            lambda x: '🎉' if ('tier5' in str(x) or 'tier6' in str(x)) else ''
                        )

                    st.dataframe(df, use_container_width=True, height=400)

                    # Download button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="💾 Download Results (CSV)",
                        data=csv,
                        file_name=f"verified_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )

    # Tab 2: Single Email
    with tab2:
        st.header("Verify Single Email")

        email = st.text_input("Enter email address", placeholder="user@example.com")

        if st.button("🔍 Verify", type="primary"):
            if email and '@' in email:
                with st.spinner("Verifying..."):
                    result = verify_email(email)

                # Display result
                status = result.get('status', 'risky')
                tier = result.get('_kadenverify_tier', '?')
                reason = result.get('_kadenverify_reason', '')

                # Check if enriched
                is_enriched = 'tier5' in reason or 'tier6' in reason
                enrichment_badge = " 🎉 **ENRICHED**" if is_enriched else ""

                if status == 'valid':
                    st.success(f"✅ Email is valid and deliverable!{enrichment_badge}")
                elif status in ['catch_all', 'risky', 'unknown']:
                    st.warning("⚠️ Email is risky - send with caution")
                    if 'risk_reason' in result:
                        st.caption(f"Reason: {result['risk_reason']}")
                elif status == 'invalid':
                    st.error("❌ Email is invalid or will bounce")
                else:
                    st.error(f"⚠️ Error: {result.get('error', 'Verification failed')}")

                # Show tier and cost
                cost = 0.0
                if 'exa' in reason:
                    cost += 0.0005
                if 'apollo' in reason:
                    cost += 0.10

                if cost > 0:
                    st.info(f"🏷️ Tier {tier} • Cost: ${cost:.4f}")
                else:
                    st.info(f"🏷️ Tier {tier} • FREE verification")

                # Show details
                with st.expander("📋 Details", expanded=True):
                    col1, col2 = st.columns(2)

                    with col1:
                        st.write("**Basic Info**")
                        st.write(f"Email: `{result.get('email')}`")
                        st.write(f"Status: `{result.get('status')}`")
                        st.write(f"Tier: `{tier}`")
                        st.write(f"Provider: `{result.get('provider', 'N/A')}`")
                        st.write(f"MX Host: `{result.get('mx_host', 'N/A')}`")

                    with col2:
                        st.write("**Flags**")
                        st.write(f"Deliverable: {'✅' if result.get('is_deliverable') else '❌'}")
                        st.write(f"Catch-all: {'⚠️' if result.get('is_catchall') else '✅'}")
                        st.write(f"Disposable: {'⚠️' if result.get('is_disposable') else '✅'}")
                        st.write(f"Role account: {'⚠️' if result.get('is_role') else '✅'}")
                        if is_enriched:
                            st.write(f"Enriched: 🎉 Yes")

                # Show JSON
                with st.expander("🔧 Raw JSON Response"):
                    st.json(result)
            else:
                st.error("Please enter a valid email address")

    # Tab 3: Batch Results
    with tab3:
        st.header("Previous Results")

        if 'results' in st.session_state:
            results = st.session_state['results']
            duration = st.session_state.get('verification_time', 0)
            tier_stats = calculate_tier_stats(results)

            st.info(f"Last verification: {len(results)} emails in {duration:.1f}s • Cost: ${tier_stats['total_cost']:.2f}")

            df = results_to_dataframe(results)

            # Add enrichment indicator
            if '_kadenverify_reason' in df.columns:
                df['enriched'] = df['_kadenverify_reason'].apply(
                    lambda x: '🎉' if ('tier5' in str(x) or 'tier6' in str(x)) else ''
                )

            # Filters
            col1, col2, col3 = st.columns(3)
            with col1:
                status_filter = st.multiselect(
                    "Filter by status",
                    options=['valid', 'risky', 'invalid', 'error'],
                    default=[]
                )
            with col2:
                provider_filter = st.multiselect(
                    "Filter by provider",
                    options=df['provider'].unique().tolist() if 'provider' in df.columns else [],
                    default=[]
                )

            # Apply filters
            filtered_df = df.copy()
            if status_filter:
                filtered_df = filtered_df[filtered_df['status'].isin(status_filter)]
            if provider_filter:
                filtered_df = filtered_df[filtered_df['provider'].isin(provider_filter)]

            # Display filtered results
            st.dataframe(filtered_df, use_container_width=True, height=500)

            # Download filtered results
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="💾 Download Filtered Results",
                data=csv,
                file_name=f"filtered_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No verification results yet. Upload a file in the 'Upload & Verify' tab.")

    # Tab 4: Database Results
    with tab4:
        st.header("💾 Database Results - All Verifications")

        from datetime import timedelta, timezone

        from engine.models import Provider
        from store.supabase_io import supabase_client_from_env

        supa = supabase_client_from_env()
        if supa is None:
            st.error(
                "Supabase store not configured. Set environment variables "
                "`KADENVERIFY_SUPABASE_URL` and `KADENVERIFY_SUPABASE_SERVICE_ROLE_KEY`."
            )
        else:
            try:
                stats = supa.get_stats()
                total = int(stats.get("total") or 0)
            except Exception as e:
                st.error(f"Supabase error: {e}")
                st.exception(e)
                total = 0
                stats = {"by_reachability": {}, "catch_all": 0, "disposable": 0}

            if total == 0:
                st.info("Supabase store is empty. Run some verifications to populate it!")
            else:
                st.success(f"📊 **{total:,} emails** verified in Supabase")

                # Date range (two lightweight queries)
                first_rows = supa.query_rows(select="verified_at", order="verified_at.asc", limit=1)
                last_rows = supa.query_rows(select="verified_at", order="verified_at.desc", limit=1)
                first_verified = first_rows[0].get("verified_at") if first_rows else None
                last_verified = last_rows[0].get("verified_at") if last_rows else None

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("First Verification", str(first_verified)[:19] if first_verified else "N/A")
                with col2:
                    st.metric("Last Verification", str(last_verified)[:19] if last_verified else "N/A")

                st.divider()

                # Summary stats
                st.subheader("📈 Summary Statistics")
                by_reachability = stats.get("by_reachability", {}) or {}
                stats_rows = []
                for reachability, count in by_reachability.items():
                    count_int = int(count or 0)
                    pct = round((count_int / total) * 100.0, 1) if total else 0.0
                    stats_rows.append(
                        {
                            "reachability": reachability,
                            "count": count_int,
                            "percentage": pct,
                        }
                    )
                stats_df = pd.DataFrame(stats_rows).sort_values("count", ascending=False) if stats_rows else pd.DataFrame(
                    columns=["reachability", "count", "percentage"]
                )

                cols = st.columns(max(1, len(stats_df)))
                for idx, row in stats_df.iterrows():
                    with cols[idx]:
                        status = row["reachability"]
                        count = row["count"]
                        pct = row["percentage"]

                        if status == "safe":
                            color = "#10b981"
                        elif status == "risky":
                            color = "#f59e0b"
                        elif status == "invalid":
                            color = "#dc2626"
                        else:
                            color = "#64748b"

                        st.markdown(
                            f"""
                            <div class="stat-box" style="background: {color};">
                                <div class="stat-number">{count}</div>
                                <div class="stat-label">{str(status).upper()} ({pct}%)</div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

                st.divider()

                # Filters
                st.subheader("🔍 Filters")

                col1, col2, col3 = st.columns(3)

                with col1:
                    status_filter_db = st.multiselect(
                        "Status",
                        options=["safe", "risky", "invalid", "unknown"],
                        default=[],
                        key="db_status_filter",
                    )

                with col2:
                    provider_options = [p.value for p in Provider]
                    provider_filter_db = st.multiselect(
                        "Provider",
                        options=provider_options,
                        default=[],
                        key="db_provider_filter",
                    )

                with col3:
                    days_back = st.selectbox(
                        "Time Period",
                        options=[
                            ("All Time", 9999),
                            ("Last 24 Hours", 1),
                            ("Last 7 Days", 7),
                            ("Last 30 Days", 30),
                        ],
                        format_func=lambda x: x[0],
                        key="db_date_filter",
                    )

                filters: dict[str, str] = {}
                if status_filter_db:
                    filters["reachability"] = f"in.({','.join(status_filter_db)})"
                if provider_filter_db:
                    filters["provider"] = f"in.({','.join(provider_filter_db)})"
                if days_back[1] < 9999:
                    start = datetime.now(timezone.utc) - timedelta(days=int(days_back[1]))
                    filters["verified_at"] = f"gt.{start.isoformat()}"

                rows = supa.query_rows(filters=filters, order="verified_at.desc", limit=10000)
                results_df = pd.DataFrame(rows)

                st.subheader(f"📋 Results ({len(results_df):,} emails)")
                st.dataframe(
                    results_df,
                    use_container_width=True,
                    height=500,
                    column_config={
                        "verified_at": st.column_config.DatetimeColumn(
                            "Verified At",
                            format="YYYY-MM-DD HH:mm:ss",
                        ),
                        "is_deliverable": st.column_config.CheckboxColumn("Deliverable"),
                        "is_catch_all": st.column_config.CheckboxColumn("Catch-all"),
                        "is_disposable": st.column_config.CheckboxColumn("Disposable"),
                        "is_role": st.column_config.CheckboxColumn("Role"),
                        "is_free": st.column_config.CheckboxColumn("Free"),
                    },
                )

                csv = results_df.to_csv(index=False)
                st.download_button(
                    label=f"💾 Download {len(results_df):,} Results (CSV)",
                    data=csv,
                    file_name=f"kadenverify_database_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

                with st.expander("📊 Provider Breakdown"):
                    provider_rows = []
                    for provider in [p.value for p in Provider]:
                        provider_total = supa.count(filters={"provider": f"eq.{provider}"})
                        if provider_total <= 0:
                            continue
                        deliverable = supa.count(
                            filters={
                                "provider": f"eq.{provider}",
                                "is_deliverable": "is.true",
                            }
                        )
                        rate = round((deliverable / provider_total) * 100.0, 1) if provider_total else 0.0
                        provider_rows.append(
                            {
                                "provider": provider,
                                "total": provider_total,
                                "deliverable": deliverable,
                                "deliverable_rate": rate,
                            }
                        )
                    provider_df = (
                        pd.DataFrame(provider_rows)
                        .sort_values("total", ascending=False)
                        .head(20)
                        if provider_rows
                        else pd.DataFrame(columns=["provider", "total", "deliverable", "deliverable_rate"])
                    )
                    st.dataframe(provider_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
