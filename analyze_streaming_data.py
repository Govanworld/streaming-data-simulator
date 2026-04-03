import sqlite3
import pandas as pd

# Connect to the database created by the generator script
DB_NAME = "streaming_data.db"

def run_business_intelligence():
    try:
        conn = sqlite3.connect(DB_NAME)
        print("--- 📊 Looper Insights: Automated KPI Report ---")

        # ANALYSIS 1: Subscription Tier Distribution
        # This shows the business how many users are on each plan
        query_subs = """
        SELECT subscription_tier, COUNT(*) as total_users
        FROM dim_users
        GROUP BY subscription_tier
        ORDER BY total_users DESC
        """
        df_subs = pd.read_sql(query_subs, conn)
        print("\n[1] User Distribution per Subscription Tier:")
        print(df_subs)

        # ANALYSIS 2: Top 5 Most Popular Genres
        # This helps the content team decide what to produce next
        query_genres = """
        SELECT t.genre, COUNT(v.view_id) as total_views
        FROM fct_views v
        JOIN dim_titles t ON v.title_id = t.title_id
        GROUP BY t.genre
        ORDER BY total_views DESC
        LIMIT 5
        """
        df_genres = pd.read_sql(query_genres, conn)
        print("\n[2] Top 5 Most Watched Genres (Market Demand):")
        print(df_genres)

        # ANALYSIS 3: Average Completion Rate per Tier
        # This identifies which user group is most engaged
        query_engagement = """
        SELECT u.subscription_tier, 
               ROUND(AVG(v.watched_percentage), 2) as avg_completion_pct
        FROM fct_views v
        JOIN dim_users u ON v.user_id = u.user_id
        GROUP BY u.subscription_tier
        """
        df_engagement = pd.read_sql(query_engagement, conn)
        print("\n[3] User Engagement (Average Completion %):")
        print(df_engagement)

        conn.close()
        print("\n--- End of Automated Report ---")
        
    except sqlite3.OperationalError:
        print("\n[!] Error: Database 'streaming_data.db' not found.")
        print("Please run 'generate_streaming_data.py' first to create the data.")

if __name__ == "__main__":
    run_business_intelligence()