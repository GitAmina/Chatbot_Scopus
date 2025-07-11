import sqlite3
import pandas as pd
from pathlib import Path
import json
import sys

DB_PATH = Path("../../data/processed/db.sqlite")
PROCESSED_DIR = Path("../../data/processed")

def list_csv_files():
    files = [f for f in PROCESSED_DIR.glob("*.csv")]
    if not files:
        print(f"Aucun fichier CSV trouvé dans {PROCESSED_DIR.resolve()}")
        sys.exit(1)
    print("\nFichiers CSV disponibles dans data/processed/:")
    for i, f in enumerate(files, 1):
        print(f"{i}. {f.name}")
    return files


def choose_file(files):
    while True:
        try:
            choice = int(input("\nEntrez le numéro du fichier CSV à importer dans la base: "))
            if 1 <= choice <= len(files):
                return files[choice - 1]
            print("Numéro invalide, veuillez réessayer.")
        except ValueError:
            print("Veuillez entrer un nombre valide.")


def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        arxiv_id TEXT UNIQUE,
        title TEXT,
        abstract TEXT,
        published TEXT,
        updated TEXT,
        domain TEXT,
        doi TEXT,
        comment TEXT,
        journal_ref TEXT,
        pdf_url TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS authors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        affiliation TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS article_authors (
        article_id INTEGER,
        author_id INTEGER,
        position INTEGER,
        PRIMARY KEY (article_id, author_id),
        FOREIGN KEY (article_id) REFERENCES articles(id),
        FOREIGN KEY (author_id) REFERENCES authors(id)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS article_categories (
        article_id INTEGER,
        category_id INTEGER,
        PRIMARY KEY (article_id, category_id),
        FOREIGN KEY (article_id) REFERENCES articles(id),
        FOREIGN KEY (category_id) REFERENCES categories(id)
    );
    """)

    conn.commit()


def get_author_id(conn, name, affiliation=None):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM authors WHERE name = ?", (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("INSERT INTO authors (name, affiliation) VALUES (?, ?)", (name, affiliation))
    conn.commit()
    return cursor.lastrowid


def get_category_id(conn, name):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM categories WHERE name = ?", (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("INSERT INTO categories (name) VALUES (?)", (name,))
    conn.commit()
    return cursor.lastrowid


def insert_article(conn, article):
    cursor = conn.cursor()
    cursor.execute("""
    INSERT OR IGNORE INTO articles (
        arxiv_id, title, abstract, published, updated, domain,
        doi, comment, journal_ref, pdf_url
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        article.get('arxiv_id'),
        article.get('title'),
        article.get('abstract'),
        article.get('published'),
        article.get('updated'),
        article.get('domain'),
        None if article.get('doi') == 'AUCUN' else article.get('doi'),
        None if article.get('comment') in ('AUCUN', 'VIDE') else article.get('comment'),
        None if article.get('journal_ref') in ('AUCUNE', 'AUCUN') else article.get('journal_ref'),
        article.get('pdf_url'),
    ))
    conn.commit()
    cursor.execute("SELECT id FROM articles WHERE arxiv_id = ?", (article.get('arxiv_id'),))
    return cursor.fetchone()[0]


def update_author_affiliations(conn):
    print("Mise à jour des affiliations (nombre d'articles par auteur)...")
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM authors")
    author_ids = [row[0] for row in cursor.fetchall()]
    total = len(author_ids)

    for i, author_id in enumerate(author_ids, 1):
        cursor.execute("""
            SELECT COUNT(*) FROM article_authors WHERE author_id = ?
        """, (author_id,))
        count = cursor.fetchone()[0]
        cursor.execute("""
            UPDATE authors SET affiliation = ? WHERE id = ?
        """, (str(count), author_id))

        # Afficher la progression toutes les 100 itérations
        if i % 100 == 0 or i == total:
            print(f"  {i}/{total} auteurs mis à jour...")

    conn.commit()
    print("Affiliations mises à jour.")

def main():
    print("=== Import des données nettoyées dans la base SQLite ===")

    files = list_csv_files()
    csv_path = choose_file(files)

    print(f"\nChargement du fichier {csv_path}...")
    df = pd.read_csv(csv_path)

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    print(f"Insertion des {len(df)} articles dans la base...")

    for idx, row in df.iterrows():
        article_dict = row.to_dict()
        article_id = insert_article(conn, article_dict)

        # === INSÉRER AUTEURS depuis la colonne 'authors' ===
        authors_raw = row.get('authors')
        if pd.notna(authors_raw):
            authors_list = [a.strip() for a in authors_raw.split(';') if a.strip()]
            for pos, name in enumerate(authors_list):
                author_id = get_author_id(conn, name)
                conn.execute("""
                INSERT OR IGNORE INTO article_authors (article_id, author_id, position)
                VALUES (?, ?, ?)
                """, (article_id, author_id, pos))
                conn.commit()

        # === INSÉRER CATÉGORIES ===
        categories_raw = row.get('categories', '')
        if pd.notna(categories_raw) and categories_raw.strip() != '':
            categories = [c.strip() for c in categories_raw.split(';') if c.strip()]
            for cat_name in categories:
                cat_id = get_category_id(conn, cat_name)
                conn.execute("""
                INSERT OR IGNORE INTO article_categories (article_id, category_id)
                VALUES (?, ?)
                """, (article_id, cat_id))
                conn.commit()

        if idx % 100 == 0 and idx > 0:
            print(f"  {idx} articles insérés...")

    # === Mise à jour des affiliations (nombre d’occurrences) ===
    update_author_affiliations(conn)

    print("Insertion terminée.")
    conn.close()


if __name__ == "__main__":
    main()
