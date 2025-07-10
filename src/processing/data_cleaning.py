import pandas as pd
import json
from pathlib import Path
import re
import sys
from datetime import datetime

class DataCleaner:
    def __init__(self):
        self.raw_dir = Path("../../data/raw")
        self.processed_dir = Path("../../data/processed")
        self.processed_dir.mkdir(exist_ok=True)

    def get_user_input(self) -> tuple:
        available_files = [f.name for f in self.raw_dir.glob("*") if f.suffix in ('.csv', '.json')]

        if not available_files:
            print("Aucun fichier CSV ou JSON trouvé dans data/raw/")
            sys.exit(1)

        print("\nFichiers disponibles dans data/raw/:")
        for i, f in enumerate(available_files, 1):
            print(f"{i}. {f}")

        while True:
            try:
                choice = int(input("\nEntrez le numéro du fichier à nettoyer: "))
                if 1 <= choice <= len(available_files):
                    input_file = available_files[choice - 1]
                    break
                print("Numéro invalide. Veuillez réessayer.")
            except ValueError:
                print("Veuillez entrer un nombre valide.")

        while True:
            output_name = input("\nEntrez le nom des fichiers de sortie (sans extension): ").strip()
            if output_name:
                break
            print("Le nom ne peut pas être vide.")

        return input_file, output_name

    def load_data(self, input_file: str) -> pd.DataFrame:
        file_path = self.raw_dir / input_file

        if file_path.suffix == '.csv':
            return pd.read_csv(file_path)
        elif file_path.suffix == '.json':
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return pd.DataFrame(data)
        else:
            raise ValueError("Format de fichier non supporté. Utilisez .csv ou .json")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df = self._remove_duplicates(df)
        df = self._clean_text_fields(df)
        df = self._normalize_domains(df)
        df = self._handle_missing_values(df)
        df = self._process_authors(df)
        df = self._process_categories(df)
        df = self._process_dates(df)

        return df

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'arxiv_id' in df.columns:
            df = df.drop_duplicates(subset=['arxiv_id'], keep='first').copy()
        else:
            df = df.drop_duplicates().copy()

        # Supprimer les lignes où arxiv_id, domain, title ou authors sont vides ou NaN
        required_fields = ['arxiv_id', 'domain', 'title', 'authors']
        for field in required_fields:
            if field in df.columns:
                df = df[df[field].notna() & (df[field].astype(str).str.strip() != '')]

        return df

    def _clean_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        text_cols = ['title', 'abstract', 'authors', 'comment', 'journal_ref']

        for col in text_cols:
            if col in df.columns:
                df.loc[:, col] = df[col].astype(str).apply(self._clean_text)

        return df

    def _clean_text(self, text: str) -> str:
        if not isinstance(text, str):
            return ""
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[^\w\s.,;:!?\'"-]', '', text)
        return text.strip()

    def _normalize_domains(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'domain' in df.columns:
            df.loc[:, 'domain'] = df['domain'].str.replace('all:', '', regex=False).str.title()
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'abstract' in df.columns:
            df.loc[:, 'abstract'] = df['abstract'].fillna('No abstract available')

        if 'doi' in df.columns:
            df.loc[:, 'doi'] = df['doi'].fillna('AUCUN')

        if 'comment' in df.columns:
            df.loc[:, 'comment'] = df['comment'].replace(['None', 'nan'], 'AUCUN').fillna('AUCUN')

        if 'journal_ref' in df.columns:
            df.loc[:, 'journal_ref'] = df['journal_ref'].replace(['None', 'nan'], 'AUCUNE').fillna('AUCUNE')

        return df

    def _process_authors(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'authors' in df.columns:
            def parse_authors(x):
                if isinstance(x, list):
                    return x
                if isinstance(x, str):
                    try:
                        return json.loads(x)
                    except json.JSONDecodeError:
                        authors_list = [a.strip() for a in x.split(';') if a.strip()]
                        return [{'name': name, 'affiliation': None} for name in authors_list]
                return []

            df.loc[:, 'authors_parsed'] = df['authors'].apply(parse_authors)

            df.loc[:, 'author_count'] = df['authors_parsed'].apply(len)

            df.loc[:, 'first_author'] = df['authors_parsed'].apply(
                lambda x: x[0]['name'] if len(x) > 0 else 'Unknown'
            )
            df.loc[:, 'last_author'] = df['authors_parsed'].apply(
                lambda x: x[-1]['name'] if len(x) > 0 else 'Unknown'
            )

        return df

    def _process_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'categories' in df.columns:
            def fix_cats(cat):
                if not isinstance(cat, str):
                    return ""

                # Cas 1: Déjà séparé par des pipes (|)
                if '|' in cat:
                    return '; '.join([c.strip() for c in cat.split('|') if c.strip()])

                # Cas 2: Déjà séparé par des points-virgules (mais mal formaté)
                if ';' in cat:
                    return '; '.join([c.strip() for c in cat.split(';') if c.strip()])

                # Cas 3: Autres formats (garder tel quel)
                return cat.strip()

            df.loc[:, 'categories'] = df['categories'].apply(fix_cats)

        return df

    def _process_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        for date_col in ['published', 'updated']:
            if date_col in df.columns:
                def format_date(d):
                    if not isinstance(d, str):
                        return ""
                    d_split = d.split(',')  # Séparation multiple si jamais
                    d1 = d_split[0].strip()
                    try:
                        dt = datetime.strptime(d1, "%Y-%m-%dT%H:%M:%SZ")
                        return dt.strftime("%Y/%m/%d %H:%M:%S")
                    except Exception:
                        return d1  # Garder tel quel si erreur

                df.loc[:, date_col] = df[date_col].apply(format_date)

        return df

    def save_clean_data(self, df: pd.DataFrame, output_name: str):
        csv_path = self.processed_dir / f"{output_name}.csv"
        json_path = self.processed_dir / f"{output_name}.json"

        # Sauvegarde CSV avec colonnes utiles
        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"\nDonnées nettoyées sauvegardées en CSV: {csv_path}")

        # Sauvegarde JSON sans authors_parsed
        """df_json = df.drop(columns=['authors_parsed'], errors='ignore')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json.loads(df_json.to_json(orient='records')), f, indent=2, ensure_ascii=False)
        print(f"Données nettoyées sauvegardées en JSON: {json_path}")"""


def main():
    print("\n=== ArXiv Data Cleaning Tool ===")

    cleaner = DataCleaner()

    input_file, output_name = cleaner.get_user_input()

    print(f"\nChargement du fichier {input_file}...")
    raw_df = cleaner.load_data(input_file)

    print("Nettoyage des données en cours...")
    clean_df = cleaner.clean_data(raw_df)

    print("\nSauvegarde des résultats...")
    cleaner.save_clean_data(clean_df, output_name)

    print("\nRésumé du traitement:")
    print(f"- Articles initiaux: {len(raw_df)}")
    print(f"- Articles après nettoyage: {len(clean_df)}")
    print(f"- Doublons supprimés: {len(raw_df) - len(clean_df)}")

    if 'authors_parsed' in clean_df.columns:
        total_authors = sum(len(authors) for authors in clean_df['authors_parsed'] if isinstance(authors, list))
        print(f"- Nombre total d'auteurs: {total_authors}")
    else:
        print("- Colonne 'authors_parsed' non trouvée")


if __name__ == "__main__":
    main()
