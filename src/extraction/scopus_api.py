import os
import requests
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
import time
import json

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
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/raw'))
        os.makedirs(base_dir, exist_ok=True)
        filepath = os.path.join(base_dir, f'{filename}.csv')
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False)
        print(f"Données sauvegardées dans {filepath}")

    def save_to_json(self, data, filename):
        """Sauvegarde les données brutes dans un fichier JSON"""
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/raw'))
        os.makedirs(base_dir, exist_ok=True)
        filepath = os.path.join(base_dir, f'{filename}.json')
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Données sauvegardées dans {filepath}")

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


def get_user_input():
    """Demande à l'utilisateur les paramètres de recherche"""
    print("\n=== Configuration de la recherche Scopus ===")

    topic = input("Entrez le sujet de recherche (ex: 'machine learning'): ").strip()
    while not topic:
        print("Erreur: Le sujet ne peut pas être vide")
        topic = input("Entrez le sujet de recherche: ").strip()

    subject = input(
        "Entrez le domaine scientifique (ex: 'COMP' pour informatique, 'BUSI' pour économie) [par défaut: COMP]: ").strip().upper()
    subject = subject if subject else "COMP"

    max_results = input("Nombre maximum de résultats à récupérer [par défaut: 50]: ").strip()
    try:
        max_results = int(max_results) if max_results else 50
    except ValueError:
        print("Valeur invalide, utilisation du défaut (50)")
        max_results = 50

    output_name = input(
        "Nom de base pour les fichiers de sortie (sans extension) [par défaut: 'scopus_results']: ").strip()
    output_name = output_name if output_name else "scopus_results"

    return {
        'topic': topic,
        'subject': subject,
        'max_results': max_results,
        'output_name': output_name
    }


def create_query(topic, subject):
    """Crée une requête Scopus valide"""
    return f"TITLE-ABS-KEY({topic}) AND SUBJAREA({subject})"


def main():
    # Obtenir les paramètres de l'utilisateur
    params = get_user_input()

    # Créer l'extracteur
    extractor = ScopusExtractor()

    # Construire la requête
    query = create_query(params['topic'], params['subject'])
    print(f"\nRequête Scopus: {query}")
    print(f"Nombre maximum de résultats: {params['max_results']}")

    # Lancer l'extraction
    results = extractor.extract_articles(query, max_results=params['max_results'])

    if results:
        # Sauvegarder les résultats
        extractor.save_to_csv(results, params['output_name'])
        extractor.save_to_json(results, params['output_name'])

        # Afficher un aperçu des données
        explore_data(results)
    else:
        print("Aucun résultat trouvé ou erreur lors de l'extraction.")


if __name__ == "__main__":
    main()