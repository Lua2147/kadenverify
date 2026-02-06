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
API_URL = "http://198.23.249.137:8025"  # RackNerd server with port 25 open
API_KEY = st.secrets.get("KADENVERIFY_API_KEY", "131245c8cc9ac8ae3d69f3a7f7e85164a29c08403483aa7b2f3608f53e5765a6")

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
            timeout=30
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


def verify_batch(emails: List[str], progress_callback=None, max_workers=20) -> List[Dict]:
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
    return [email_to_result.get(email, {"email": email, "status": "error", "error": "not processed"})
            for email in emails]


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


def results_to_dataframe(results: List[Dict]) -> pd.DataFrame:
    """Convert results to pandas DataFrame."""
    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)

    # Reorder columns for better display
    priority_cols = ['email', 'status', 'reachability', 'is_deliverable',
                     'is_catchall', 'catchall_confidence', 'is_disposable',
                     'is_role', 'provider', 'mx_host']

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
        **KadenVerify** verifies emails using:
        - SMTP handshake
        - DNS MX lookup
        - Catch-all detection
        - Pattern matching

        **Status meanings:**
        - ✅ **valid** - Deliverable
        - ⚠️ **catch_all** - Risky
        - ❌ **invalid** - Bounces
        - ❓ **unknown** - Unclear
        """)

    # Main content
    tab1, tab2, tab3 = st.tabs(["📁 Upload & Verify", "⚡ Single Email", "📊 Batch Results"])

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

                    def update_progress(current, total):
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

                    col1, col2, col3, col4 = st.columns(4)

                    safe_count = len(df[df['status'] == 'valid'])
                    risky_count = len(df[df['status'].isin(['catch_all', 'risky'])])
                    invalid_count = len(df[df['status'] == 'invalid'])
                    unknown_count = len(df[df['status'] == 'unknown'])

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
                            <div class="stat-number">{unknown_count}</div>
                            <div class="stat-label">❓ Unknown</div>
                        </div>
                        """, unsafe_allow_html=True)

                    st.divider()

                    # Show results table
                    st.subheader("📋 Results")
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
                status = result.get('status', 'unknown')

                if status == 'valid':
                    st.success("✅ Email is valid and deliverable!")
                elif status in ['catch_all', 'risky']:
                    st.warning("⚠️ Email is risky (catch-all domain or other issues)")
                elif status == 'invalid':
                    st.error("❌ Email is invalid or will bounce")
                else:
                    st.info("❓ Could not determine email status")

                # Show details
                with st.expander("📋 Details", expanded=True):
                    col1, col2 = st.columns(2)

                    with col1:
                        st.write("**Basic Info**")
                        st.write(f"Email: `{result.get('email')}`")
                        st.write(f"Status: `{result.get('status')}`")
                        st.write(f"Provider: `{result.get('provider', 'N/A')}`")
                        st.write(f"MX Host: `{result.get('mx_host', 'N/A')}`")

                    with col2:
                        st.write("**Flags**")
                        st.write(f"Deliverable: {'✅' if result.get('is_deliverable') else '❌'}")
                        st.write(f"Catch-all: {'⚠️' if result.get('is_catchall') else '✅'}")
                        st.write(f"Disposable: {'⚠️' if result.get('is_disposable') else '✅'}")
                        st.write(f"Role account: {'⚠️' if result.get('is_role') else '✅'}")

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

            st.info(f"Last verification: {len(results)} emails in {duration:.1f}s")

            df = results_to_dataframe(results)

            # Filters
            col1, col2, col3 = st.columns(3)
            with col1:
                status_filter = st.multiselect(
                    "Filter by status",
                    options=['valid', 'catch_all', 'risky', 'invalid', 'unknown'],
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


if __name__ == "__main__":
    main()
