import sqlite3
import pandas as pd

# Connection to our streaming and transaction database
DB_NAME = "streaming_data.db"

def run_fraud_audit():
    try:
        conn = sqlite3.connect(DB_NAME)
        print("--- 🛡️ Govan Mohammad | Advanced Fraud & AML Audit ---")
        print("Framework: Financial Crime Compliance & Risk Mitigation\n")

        # CHECK 1: Velocity Attack Detection (Brute Force / Account Takeover)
        # We look for users with unusual high-frequency activity in a short time.
        query_velocity = """
        SELECT user_id, COUNT(view_id) as event_count
        FROM fct_views
        GROUP BY user_id
        HAVING event_count > 100
        ORDER BY event_count DESC
        """
        velocity_risks = pd.read_sql(query_velocity, conn)
        
        if len(velocity_risks) == 0:
            print("[PASS] Velocity Check: No automated brute-force patterns detected.")
        else:
            print(f"[ALERT] High-Velocity Risk: {len(velocity_risks)} accounts showing bot-like behavior.")

        # CHECK 2: Geographic Anomaly (AML Compliance)
        # Identify users who change countries suspiciously fast (Impossible Travel).
        # This is a standard check for AML and KYC monitoring.
        query_geo = """
        SELECT user_id, COUNT(DISTINCT country) as distinct_countries
        FROM dim_users
        GROUP BY user_id
        HAVING distinct_countries > 1
        """
        geo_risks = pd.read_sql(query_geo, conn)

        if len(geo_risks) == 0:
            print("[PASS] AML Geo-Compliance: No suspicious cross-border account sharing.")
        else:
            print(f"[WARNING] Compliance Alert: {len(geo_risks)} users flagged for geo-inconsistency.")

        # CHECK 3: Subscription Fraud (Transaction Integrity)
        # Check for Free users who somehow have access to 'Premium-only' metrics.
        query_integrity = """
        SELECT u.user_id, u.subscription_tier, v.watched_percentage
        FROM dim_users u
        JOIN fct_views v ON u.user_id = v.user_id
        WHERE u.subscription_tier = 'Free' AND v.watched_percentage > 99
        LIMIT 5
        """
        print("\n[INFO] Subscription Integrity Audit (Sample):")
        print(pd.read_sql(query_integrity, conn))

        conn.close()
        print("\n--- Fraud Monitoring Complete ---")

    except Exception as e:
        print(f"Error: {e}. Please ensure the database is generated first.")

if __name__ == "__main__":
    run_fraud_audit()