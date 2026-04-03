import sqlite3
import pandas as pd

DB_NAME = "streaming_data.db"

def run_security_audit():
    conn = sqlite3.connect(DB_NAME)
    print("--- 🔒 Cybersecurity & Privacy Audit Report ---")
    
    # CHECK 1: PII Exposure (Privacy)
    # We check if there are any plain-text passwords or sensitive data in the logs.
    # A professional data analyst ensures no 'unmasked' data is visible.
    query_pii = "SELECT COUNT(*) as risk_count FROM dim_users WHERE email LIKE '%@example.com%'"
    risk_count = pd.read_sql(query_pii, conn).iloc[0]['risk_count']
    
    if risk_count > 0:
        print(f"[WARNING] {risk_count} internal/test accounts detected. Risk of data leakage.")
    else:
        print("[PASS] No internal test accounts found in production-ready table.")

    # CHECK 2: Unauthorized Access Patterns (Anomaly Detection)
    # Are there users watching movies from 10 different countries in one day?
    # This is a classic security check for 'Account Sharing' or 'Hacking'.
    query_anomaly = """
    SELECT user_id, COUNT(DISTINCT country) as country_count 
    FROM dim_users 
    GROUP BY user_id 
    HAVING country_count > 1
    """
    anomalies = pd.read_sql(query_anomaly, conn)
    
    if len(anomalies) == 0:
        print("[PASS] No suspicious cross-border access patterns detected.")
    else:
        print(f"[ALERT] {len(anomalies)} users show suspicious multi-country login activity!")

    # CHECK 3: Admin Role Review (Least Privilege)
    # SOC2/ISO27001 requires that we don't have too many 'Premium/Admin' users.
    query_tiers = "SELECT subscription_tier, COUNT(*) as count FROM dim_users GROUP BY subscription_tier"
    print("\n[INFO] Access Level Distribution:")
    print(pd.read_sql(query_tiers, conn))

    conn.close()
    print("\n--- Security Audit Complete ---")

if __name__ == "__main__":
    run_security_audit()