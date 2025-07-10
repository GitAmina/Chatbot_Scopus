import sqlite3
from pathlib import Path
import pandas as pd
from typing import Dict

class ArXivDatabase:
    def __init__(self, db_path: str = "../../data/processed/arxiv_db.sqlite"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)
        self.conn = None

    def connect(self):
        """Établit une connexion à la base de données"""
        self.conn = sqlite3.connect(self.db_path)
        return self.conn

    def close(self):
        """Ferme la connexion à la base de données"""
        if self.conn:
            self.conn.close()

    def initialize_database(self):
        """Initialise la structure de la base de données"""
        with self.connect() as conn:
            cursor = conn.cursor()

            # Table Articles
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    arxiv_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    abstract TEXT,
                    published TEXT,
                    updated TEXT,
                    primary_category TEXT,
                    categories TEXT,
                    pdf_url TEXT,
                    doi TEXT,
                    comment TEXT,
                    journal_ref TEXT,
                    domain TEXT,
                    author_count INTEGER,
                    first_author TEXT
                )
            """)

            # Table Authors
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS authors (
                    name TEXT PRIMARY KEY,
                    affiliation TEXT
                )
            """)

            # Table de jointure Article-Author
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS author_article (
                    arxiv_id TEXT,
                    author_name TEXT,
                    FOREIGN KEY (arxiv_id) REFERENCES articles (arxiv_id),
                    FOREIGN KEY (author_name) REFERENCES authors (name),
                    PRIMARY KEY (arxiv_id, author_name)
                )
            """)

            conn.commit()

    def insert_data(self, tables: Dict[str, pd.DataFrame]):
        """Insère les données dans la base"""
        with self.connect() as conn:
            # Insertion des articles
            tables['articles'].to_sql(
                'articles', conn,
                if_exists='replace',
                index=False
            )

            # Insertion des auteurs
            tables['authors'].to_sql(
                'authors', conn,
                if_exists='replace',
                index=False
            )

            # Insertion des relations auteur-article
            tables['author_article'].to_sql(
                'author_article', conn,
                if_exists='replace',
                index=False
            )

    def query(self, sql: str, params=None) -> pd.DataFrame:
        """Exécute une requête SQL et retourne un DataFrame"""
        with self.connect() as conn:
            return pd.read_sql(sql, conn, params=params)


def test_database():
    """Teste la base de données avec des requêtes simples"""
    db = ArXivDatabase()
    db.initialize_database()

    # Exemple de requête
    print("\n5 premiers articles:")
    print(db.query("SELECT title, first_author FROM articles LIMIT 5"))

    print("\nNombre d'articles par domaine:")
    print(db.query("""
        SELECT domain, COUNT(*) as count 
        FROM articles 
        GROUP BY domain 
        ORDER BY count DESC
    """))

    print("\nAuteurs les plus prolifiques:")
    print(db.query("""
        SELECT a.name, COUNT(aa.arxiv_id) as article_count
        FROM authors a
        JOIN author_article aa ON a.name = aa.author_name
        GROUP BY a.name
        ORDER BY article_count DESC
        LIMIT 5
    """))

    db.close()


if __name__ == "__main__":
    # Exemple d'utilisation
    from data_cleaning import DataCleaner

    # Nettoyage des données
    cleaner = DataCleaner("../../data/raw/arxiv_combined.json")
    clean_df = cleaner.clean_data(cleaner.load_data())
    db_tables = cleaner.prepare_for_database(clean_df)

    # Initialisation de la base de données
    db = ArXivDatabase()
    db.initialize_database()
    db.insert_data(db_tables)

    # Test des requêtes
    test_database()