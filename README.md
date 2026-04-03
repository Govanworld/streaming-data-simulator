# 📺 Streaming Service Data Simulator

A Python-based data engineering project that simulates real-world streaming platform data. This tool generates thousands of synthetic records for users, content metadata, and viewing events, then structures them into a relational SQL database.

## 🚀 Overview
This project was built to demonstrate how to handle high-volume event data (like Netflix or Disney+) and prepare it for advanced analytics. It follows a **Star Schema** architecture, making it ready for BI tools like Tableau or Looker Studio.

## 🛠️ Tech Stack
*   **Language:** Python 3.x
*   **Libraries:** `Pandas` (Data manipulation), `Faker` (Synthetic data generation)
*   **Database:** SQLite / SQL
*   **Architecture:** Medallion-style (Raw to Structured)

## 📊 Data Model
The script generates three main tables:
1.  **`dim_users`**: Subscriber details (ID, Email, Subscription Tier, Country).
2.  **`dim_titles`**: Content catalog (Title, Genre, Release Year).
3.  **`fct_views`**: The "Heart" of the data. Every time a user watches a show, a record is created with timestamps and completion percentages.

## 🛡️ Security & Compliance (SOC2/ISO27001)
Even though the data is synthetic, the script is designed with data privacy in mind:
*   **No PII:** Uses `Faker` to ensure no real-world personal data is used.
*   **Reproducible:** Clean code logic ensures that data pipelines can be audited and re-run.

## 📖 How to Use
1. Clone the repo.
2. Install dependencies: `pip install faker pandas`.
3. Run the script: `python generate_streaming_data.py`.
4. Open the generated `.db` file in any SQL client to start querying!
