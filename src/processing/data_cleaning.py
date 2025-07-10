import pandas as pd
import numpy as np
import re
import sqlite3
from pathlib import Path
import json
from tqdm import tqdm
import logging

# Configuration
from pathlib import Path

# Chemins de base
CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent.parent
DATA_DIR = ROOT_DIR / 'data'
RAW_DIR = DATA_DIR / 'raw'
PROCESSED_DIR = DATA_DIR / 'processed'
DB_PATH = PROCESSED_DIR / 'scopus.sqlite'

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCleaner:
    def __init__(self):
        self.processed_dir = PROCESSED_DIR
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def load_data(self, input_file):
        """Charge les données depuis un fichier JSON ou CSV"""
        input_path = RAW_DIR / input_file

        if input_path.suffix == '.json':
            with open(input_path, 'r', encoding='utf-8') as f:
                return pd.DataFrame(json.load(f))
        else:
            return pd.read_csv(input_path)

    def clean_text(self, text):
        """Nettoie les textes des caractères spéciaux"""
        if pd.isna(text):
            return text

        replacements = [
            ('\n', ' '), ('\r', ' '), ('\t', ' '),
            ('&amp;', '&'), ('&quot;', '"'), ('&gt;', '>'),
            ('&lt;', '<'), ('&#39;', "'")
        ]

        cleaned = str(text)
        for old, new in replacements:
            cleaned = cleaned.replace(old, new)

        return cleaned.strip()

    def extract_authors_info(self, authors_entry):
        """Extrait et nettoie les informations des auteurs"""
        if pd.isna(authors_entry):
            return []

        authors = []
        if isinstance(authors_entry, str):
            try:
                authors_entry = json.loads(authors_entry.replace("'", '"'))
            except:
                return []

        for author in authors_entry:
            try:
                author_info = {
                    'scopus_id': author.get('@auid', ''),
                    'orcid': author.get('@orcid', ''),
                    'given_name': author.get('preferred-name', {}).get('given-name', ''),
                    'surname': author.get('preferred-name', {}).get('surname', ''),
                    'affiliation_id': author.get('affiliation', [{}])[0].get('@id', ''),
                    'affiliation_name': author.get('affiliation', [{}])[0].get('@name', '')
                }
                authors.append(author_info)
            except:
                continue

        return authors

    def clean_dataframe(self, df):
        """Nettoie le dataframe principal"""
        logger.info("Nettoyage des données...")

        # Suppression des doublons
        df = df.drop_duplicates(subset=['dc:identifier', 'prism:doi'], keep='first')

        # Nettoyage des textes
        text_cols = ['dc:title', 'dc:description', 'prism:publicationName', 'authkeywords']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].apply(self.clean_text)

        # Gestion des valeurs manquantes
        df['dc:description'] = df['dc:description'].fillna('Abstract not available')
        df['authkeywords'] = df['authkeywords'].fillna('No keywords')

        # Extraction de l'année
        df['year'] = pd.to_datetime(df['prism:coverDate']).dt.year

        # Nettoyage des domaines
        if 'subject-area' in df.columns:
            df['subject-area'] = df['subject-area'].apply(
                lambda x: [area['@abbrev'] for area in x] if isinstance(x, list) else []
            )

        return df

    def create_database_schema(self, conn):
        """Crée la structure de la base de données"""
        cursor = conn.cursor()

        # Table Articles
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scopus_id TEXT UNIQUE,
            doi TEXT UNIQUE,
            title TEXT,
            abstract TEXT,
            publication_date TEXT,
            publication_name TEXT,
            keywords TEXT,
            subject_areas TEXT
        )
        """)

        # Table Authors
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS authors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scopus_id TEXT UNIQUE,
            orcid TEXT,
            given_name TEXT,
            surname TEXT
        )
        """)

        # Table Affiliations
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS affiliations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scopus_id TEXT UNIQUE,
            name TEXT,
            country TEXT
        )
        """)

        # Table de jointure Article-Author
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS article_author (
            article_id INTEGER,
            author_id INTEGER,
            PRIMARY KEY (article_id, author_id),
            FOREIGN KEY (article_id) REFERENCES articles(id),
            FOREIGN KEY (author_id) REFERENCES authors(id)
        )
        """)

        # Table de jointure Author-Affiliation
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS author_affiliation (
            author_id INTEGER,
            affiliation_id INTEGER,
            PRIMARY KEY (author_id, affiliation_id),
            FOREIGN KEY (author_id) REFERENCES authors(id),
            FOREIGN KEY (affiliation_id) REFERENCES affiliations(id)
        )
        """)

        conn.commit()

    def save_to_database(self, df):
        """Sauvegarde les données nettoyées dans SQLite"""
        logger.info("Sauvegarde dans la base de données...")
        conn = sqlite3.connect(DB_PATH)
        self.create_database_schema(conn)

        # Insertion des articles
        articles = df[[
            'dc:identifier', 'prism:doi', 'dc:title',
            'dc:description', 'prism:coverDate',
            'prism:publicationName', 'authkeywords', 'subject-area'
        ]].copy()

        articles.columns = [
            'scopus_id', 'doi', 'title', 'abstract',
            'publication_date', 'publication_name', 'keywords', 'subject_areas'
        ]

        articles['subject_areas'] = articles['subject_areas'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else ''
        )

        articles.to_sql('articles', conn, if_exists='append', index=False)

        # Récupération des IDs des articles insérés
        article_ids = pd.read_sql("SELECT id, scopus_id FROM articles", conn)

        # Traitement des auteurs et affiliations
        authors_data = []
        affiliations_data = []
        article_author_links = []
        author_affiliation_links = []

        for _, row in tqdm(df.iterrows(), total=len(df), desc="Traitement des auteurs"):
            article_scopus_id = row['dc:identifier']
            article_id = article_ids[article_ids['scopus_id'] == article_scopus_id]['id'].values[0]

            authors = self.extract_authors_info(row.get('author', []))

            for author in authors:
                # Ajout auteur
                authors_data.append((
                    author['scopus_id'], author['orcid'],
                    author['given_name'], author['surname']
                ))

                # Ajout affiliation si existe
                if author['affiliation_id']:
                    affiliations_data.append((
                        author['affiliation_id'], author['affiliation_name'], ''
                    ))

                # Liens
                author_idx = len(authors_data) - 1
                article_author_links.append((article_id, author_idx + 1))  # +1 car SQLite commence à 1

                if author['affiliation_id']:
                    aff_idx = len(affiliations_data) - 1
                    author_affiliation_links.append((author_idx + 1, aff_idx + 1))

        # Insertion en masse
        pd.DataFrame(authors_data, columns=[
            'scopus_id', 'orcid', 'given_name', 'surname'
        ]).drop_duplicates('scopus_id').to_sql('authors', conn, if_exists='append', index=False)

        pd.DataFrame(affiliations_data, columns=[
            'scopus_id', 'name', 'country'
        ]).drop_duplicates('scopus_id').to_sql('affiliations', conn, if_exists='append', index=False)

        # Insertion des liens
        pd.DataFrame(article_author_links, columns=[
            'article_id', 'author_id'
        ]).to_sql('article_author', conn, if_exists='append', index=False)

        pd.DataFrame(author_affiliation_links, columns=[
            'author_id', 'affiliation_id'
        ]).to_sql('author_affiliation', conn, if_exists='append', index=False)

        conn.close()
        logger.info(f"Base de données sauvegardée: {DB_PATH}")

    def save_to_csv(self, df):
        """Sauvegarde les données nettoyées en CSV"""
        csv_path = self.processed_dir / 'cleaned_data.csv'
        df.to_csv(csv_path, index=False)
        logger.info(f"Données CSV nettoyées sauvegardées: {csv_path}")


def main():
    cleaner = DataCleaner()

    # Chargement des données
    input_file = input("Entrez le nom du fichier à nettoyer (depuis data/raw/): ")
    df = cleaner.load_data(input_file)

    # Nettoyage
    cleaned_df = cleaner.clean_dataframe(df)

    # Sauvegarde
    cleaner.save_to_csv(cleaned_df)
    cleaner.save_to_database(cleaned_df)

    logger.info("Nettoyage terminé avec succès!")


if __name__ == "__main__":
    main()