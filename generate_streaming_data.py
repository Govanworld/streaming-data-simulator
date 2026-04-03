import sqlite3
import pandas as pd
from faker import Faker
import random

# --- CONFIGURATIE ---
NUM_USERS = 1000        
NUM_TITLES = 50         
NUM_VIEW_EVENTS = 5000 

fake = Faker('en_US') # Engels voor de Londense vacature!
DB_NAME = "streaming_data.db"

def main():
    print("--- Generating Synthetic Streaming Data ---")
    
    # 1. Users
    users = []
    for i in range(NUM_USERS):
        users.append({
            'user_id': i + 1,
            'name': fake.name(),
            'email': fake.unique.email(),
            'subscription_tier': random.choice(['Free', 'Basic', 'Premium']),
            'country': fake.country_code(),
            'signup_date': fake.date_between(start_date='-2y', end_date='today')
        })
    df_users = pd.DataFrame(users)

    # 2. Content Titles
    genres = ['Action', 'Comedy', 'Drama', 'Sci-Fi', 'Documentary']
    titles = []
    for i in range(NUM_TITLES):
        titles.append({
            'title_id': i + 1,
            'title_name': fake.catch_phrase(),
            'genre': random.choice(genres),
            'release_year': random.randint(2015, 2024)
        })
    df_titles = pd.DataFrame(titles)

    # 3. View Events (The Fact Table)
    views = []
    for _ in range(NUM_VIEW_EVENTS):
        percent = random.randint(1, 100)
        views.append({
            'view_id': fake.uuid4(),
            'user_id': random.randint(1, NUM_USERS),
            'title_id': random.randint(1, NUM_TITLES),
            'view_date': fake.date_time_between(start_date='-6m', end_date='now'),
            'watched_percentage': percent,
            'is_completed': percent >= 90
        })
    df_views = pd.DataFrame(views)

    # --- SAVE TO SQL ---
    conn = sqlite3.connect(DB_NAME)
    df_users.to_sql('dim_users', conn, if_exists='replace', index=False)
    df_titles.to_sql('dim_titles', conn, if_exists='replace', index=False)
    df_views.to_sql('fct_views', conn, if_exists='replace', index=False)
    conn.close()
    
    print(f"Success! Created {DB_NAME} with 3 tables.")

if __name__ == "__main__":
    main()