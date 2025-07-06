#src/extraction/scopus_api.py

import os
import requests
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
import time

# Charger la clé API depuis .env
load_dotenv()
API_KEY = os.getenv('SCOPUS_API_KEY')


class ScopusExtractor:
    BASE_URL = "https://api.elsevier.com/content/search/scopus"

    def __init__(self, api_key=API_KEY):
        self.api_key = api_key
        self.headers = {
            'Accept': 'application/json',
            'X-ELS-APIKey': self.api_key
        }

    def search_articles(self, query, count=25, start=0):
        """Recherche des articles sur Scopus"""
        params = {
            'query': query,
            'count': count,
            'start': start,
            'field': 'title,abstract,authors,coverDate,publicationName,doi,authorKeywords,subject-area'
        }

        try:
            response = requests.get(self.BASE_URL, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la requête: {e}")
            return None

    def extract_articles(self, query, max_results=100):
        """Extrait plusieurs pages de résultats"""
        all_results = []
        batch_size = 25  # Scopus limite à 25 résultats par requête
        max_pages = (max_results + batch_size - 1) // batch_size

        for page in tqdm(range(max_pages), desc="Extraction des articles"):
            start = page * batch_size
            data = self.search_articles(query, count=batch_size, start=start)

            if data and 'search-results' in data and 'entry' in data['search-results']:
                all_results.extend(data['search-results']['entry'])
                time.sleep(0.5)  # Respecter les limites de taux d'appel

            if len(all_results) >= max_results:
                break

        return all_results[:max_results]

    def save_to_csv(self, data, filename):
        """Sauvegarde les données dans un fichier CSV"""
        df = pd.DataFrame(data)
        os.makedirs('../../data/raw', exist_ok=True)
        df.to_csv(f'../../data/raw/{filename}.csv', index=False)
        print(f"Données sauvegardées dans data/raw/{filename}.csv")

    def save_to_json(self, data, filename):
        """Sauvegarde les données brutes dans un fichier JSON"""
        import json
        os.makedirs('../../data/raw', exist_ok=True)
        with open(f'../../data/raw/{filename}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Données sauvegardées dans data/raw/{filename}.json")

if __name__ == "__main__":
    extractor = ScopusExtractor()

    # Exemple de requête pour les articles sur "machine learning" en informatique
    query = "TITLE-ABS-KEY(machine learning) AND SUBJAREA(COMP)"
    results = extractor.extract_articles(query, max_results=50)

    if results:
        extractor.save_to_csv(results, 'machine_learning_articles')
        extractor.save_to_json(results, 'machine_learning_articles')
    else:
        print("Aucun résultat trouvé ou erreur lors de l'extraction.")


def explore_data(data):
    """Affiche quelques informations sur les données extraites"""
    if not data:
        print("Aucune donnée à explorer")
        return

    df = pd.DataFrame(data)

    print("\n=== Exploration des données ===")
    print(f"Nombre d'articles: {len(df)}")

    if 'prism:publicationName' in df.columns:
        print("\nRevues les plus fréquentes:")
        print(df['prism:publicationName'].value_counts().head(5))

    if 'prism:coverDate' in df.columns:
        print("\nDistribution par année:")
        df['year'] = df['prism:coverDate'].str[:4]
        print(df['year'].value_counts().sort_index())

    if 'dc:title' in df.columns:
        print("\nExemple de titres:")
        for title in df['dc:title'].head(3):
            print(f"- {title}")


# Dans le bloc __main__, après l'extraction
if results:
    explore_data(results)