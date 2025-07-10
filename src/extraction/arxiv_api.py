import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path
import json
from xml.etree import ElementTree as ET
import time

class ArXivExtractor:
    def __init__(self):
        self.base_url = "http://export.arxiv.org/api/query"
        self.max_batch_size = 1000  # Nombre max d'articles par requête
        self.delay = 3  # Délai entre les requêtes (en secondes)

    def get_all_metadata(self, search_query, max_results=30000):
        """Récupère les métadonnées avec pagination automatique"""
        all_articles = []
        total_retrieved = 0

        with tqdm(total=max_results, desc=f"Recherche: {search_query[:30]}") as pbar:
            while total_retrieved < max_results:
                batch_size = min(self.max_batch_size, max_results - total_retrieved)

                params = {
                    'search_query': search_query,
                    'start': total_retrieved,
                    'max_results': batch_size,
                    'sortBy': 'submittedDate',
                    'sortOrder': 'descending'
                }

                try:
                    response = requests.get(self.base_url, params=params, timeout=30)
                    response.raise_for_status()
                    articles = self._parse_full_xml(response.text, search_query)

                    if not articles:
                        break  # Plus d'articles disponibles

                    all_articles.extend(articles)
                    total_retrieved += len(articles)
                    pbar.update(len(articles))

                    time.sleep(self.delay)  # Respect du délai

                except Exception as e:
                    print(f"\nErreur lors de la récupération: {str(e)}")
                    break

        return all_articles[:max_results]

    def _parse_full_xml(self, xml_text, search_query):
        """Analyse le XML et extrait les métadonnées"""
        articles = []
        root = ET.fromstring(xml_text)

        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            article = {
                'arxiv_id': entry.find('{http://www.w3.org/2005/Atom}id').text.split('/')[-1],
                'published': entry.find('{http://www.w3.org/2005/Atom}published').text,
                'updated': entry.find('{http://www.w3.org/2005/Atom}updated').text,
                'title': self._clean_text(entry.find('{http://www.w3.org/2005/Atom}title').text),
                'summary': self._clean_text(entry.find('{http://www.w3.org/2005/Atom}summary').text),
                'authors': [self._parse_author(author) for author in
                            entry.findall('{http://www.w3.org/2005/Atom}author')],
                'primary_category': entry.find('{http://arxiv.org/schemas/atom}primary_category').attrib['term'],
                'categories': [cat.attrib['term'] for cat in entry.findall('{http://www.w3.org/2005/Atom}category')],
                'pdf_url': self._find_pdf_link(entry),
                'doi': self._find_doi(entry),
                'comment': entry.find('{http://arxiv.org/schemas/atom}comment').text if entry.find(
                    '{http://arxiv.org/schemas/atom}comment') is not None else None,
                'journal_ref': entry.find('{http://arxiv.org/schemas/atom}journal_ref').text if entry.find(
                    '{http://arxiv.org/schemas/atom}journal_ref') is not None else None,
                'search_query': search_query  # Ajoute la requête originale
            }
            articles.append(article)

        return articles

    def _parse_author(self, author_element):
        """Extrait les informations d'un auteur"""
        return {
            'name': author_element.find('{http://www.w3.org/2005/Atom}name').text,
            'affiliation': author_element.find('{http://www.w3.org/2005/Atom}affiliation').text
            if author_element.find('{http://www.w3.org/2005/Atom}affiliation') is not None else None
        }

    def _find_pdf_link(self, entry):
        """Trouve le lien vers le PDF"""
        for link in entry.findall('{http://www.w3.org/2005/Atom}link'):
            if 'title' in link.attrib and link.attrib['title'] == 'pdf':
                return link.attrib['href']
        return None

    def _find_doi(self, entry):
        """Trouve le DOI si disponible"""
        for id_element in entry.findall('{http://arxiv.org/schemas/atom}doi'):
            return id_element.text
        return None

    def _clean_text(self, text):
        """Nettoie le texte des caractères indésirables"""
        return text.replace('\n', ' ').strip() if text else ""

    def save_combined_data(self, all_articles, filename):
        """Sauvegarde tous les résultats dans un seul fichier"""
        output_dir = Path('../../data/raw')
        output_dir.mkdir(parents=True, exist_ok=True)

        # Sauvegarde JSON
        """ json_path = output_dir / f'{filename}.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(all_articles, f, indent=2, ensure_ascii=False)
        print(f"\nFichier JSON sauvegardé : {json_path}")"""

        # Conversion pour CSV
        csv_data = []
        for article in all_articles:
            csv_data.append({
                'arxiv_id': article['arxiv_id'],
                'domain': article['search_query'],
                'title': article['title'],
                'abstract': article['summary'],
                'published': article['published'],
                'updated': article['updated'],
                'authors': '; '.join([f"{a['name']}" + (f" ({a['affiliation']})" if a['affiliation'] else "")
                                      for a in article['authors']]),
                'primary_category': article['primary_category'],
                'categories': '|'.join(article['categories']),
                'pdf_url': article['pdf_url'],
                'doi': article['doi'],
                'comment': article['comment'],
                'journal_ref': article['journal_ref']
            })

        # Sauvegarde CSV
        csv_path = output_dir / f'{filename}.csv'
        pd.DataFrame(csv_data).to_csv(csv_path, index=False, encoding='utf-8')
        print(f"Fichier CSV sauvegardé : {csv_path}")


def main():
    print("\n=== Extracteur arXiv Multi-Domaines ===")
    print("Recherchez plusieurs domaines et sauvegardez tout dans un seul fichier\n")

    # Nom du fichier de sortie
    filename = input("Entrez le nom du fichier de sortie (sans extension) [arxiv_combined]: ").strip()
    filename = filename or "arxiv_combined"

    extractor = ArXivExtractor()
    all_results = []

    while True:
        # Saisie du domaine
        query = input("\nEntrez un domaine à rechercher (ex: 'machine learning'): ").strip()
        while not query:
            query = input("Veuillez entrer un domaine valide: ").strip()

        # Nombre d'articles
        max_results = input("Nombre d'articles à récupérer [100 par défaut]: ").strip()
        try:
            max_results = min(30000, int(max_results)) if max_results else 100
        except ValueError:
            print("Nombre invalide. Utilisation de 100 par défaut.")
            max_results = 100

        # Récupération des articles
        articles = extractor.get_all_metadata(f"all:{query}", max_results)

        if articles:
            all_results.extend(articles)
            print(f"→ {len(articles)} articles ajoutés (Total: {len(all_results)})")
        else:
            print("Aucun article trouvé pour ce domaine.")

        # Demande de continuation
        continuer = input("\nVoulez-vous rechercher un autre domaine ? (oui/non): ").strip().lower()
        while continuer not in ['oui', 'non', 'o', 'n']:
            continuer = input("Répondez 'oui' ou 'non': ").strip().lower()

        if continuer in ['non', 'n']:
            break

    # Sauvegarde finale
    if all_results:
        extractor.save_combined_data(all_results, filename)
        print("\nRésumé final:")
        print(f"- Articles totaux: {len(all_results)}")
        print(f"- Domaines différents: {len(set(a['search_query'] for a in all_results))}")
    else:
        print("\nAucune donnée à sauvegarder.")


if __name__ == "__main__":
    main()