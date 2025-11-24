import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chemins
script_dir = Path(__file__).parent
project_root = script_dir.parent
BATCHES_DIR = project_root / "data" / "output_batches"
OUTPUT_DIR = project_root / "data" / "merged_output"

def validate_batch_file(batch_path):
    """Valide qu'un fichier batch est lisible et non vide"""
    try:
        df = pd.read_csv(batch_path, encoding='utf-8-sig', nrows=5)
        if len(df) == 0:
            logger.warning(f"Batch vide détecté: {batch_path.name}")
            return False
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la lecture de {batch_path.name}: {e}")
        return False

def merge_all_batches():
    """Fusionne tous les fichiers batch en un seul DataFrame"""
    
    logger.info("="*60)
    logger.info("DÉMARRAGE DU MERGE DES BATCHES")
    logger.info("="*60)
    
    # Vérifier que le dossier existe
    if not BATCHES_DIR.exists():
        logger.error(f"Le dossier {BATCHES_DIR} n'existe pas!")
        return None
    
    # Récupérer tous les fichiers batch (triés par ordre numérique)
    batch_files = sorted(BATCHES_DIR.glob("batch_*.csv"))
    
    if not batch_files:
        logger.error("Aucun fichier batch trouvé!")
        return None
    
    logger.info(f"Nombre de fichiers batch trouvés: {len(batch_files)}")
    
    # Valider tous les batches avant de commencer
    logger.info("\n--- VALIDATION DES BATCHES ---")
    valid_batches = []
    for batch_path in batch_files:
        if validate_batch_file(batch_path):
            valid_batches.append(batch_path)
            logger.info(f"✓ {batch_path.name} - OK")
        else:
            logger.warning(f"✗ {batch_path.name} - SKIP")
    
    if not valid_batches:
        logger.error("Aucun batch valide trouvé!")
        return None
    
    logger.info(f"\nBatches valides: {len(valid_batches)}/{len(batch_files)}")
    
    # Charger et concaténer tous les batches
    logger.info("\n--- CHARGEMENT DES DONNÉES ---")
    dfs = []
    total_rows = 0
    
    for batch_path in valid_batches:
        try:
            df = pd.read_csv(batch_path, encoding='utf-8-sig')
            rows = len(df)
            total_rows += rows
            dfs.append(df)
            logger.info(f"Chargé {batch_path.name}: {rows:,} lignes")
        except Exception as e:
            logger.error(f"Erreur lors du chargement de {batch_path.name}: {e}")
            continue
    
    if not dfs:
        logger.error("Aucune donnée chargée!")
        return None
    
    # Analyser les colonnes de chaque batch
    logger.info("\n--- ANALYSE DES COLONNES ---")
    all_columns = set()
    batch_column_counts = {}
    
    for i, df in enumerate(dfs):
        batch_num = i + 1
        cols = set(df.columns)
        all_columns.update(cols)
        batch_column_counts[batch_num] = len(cols)
        logger.info(f"Batch {batch_num}: {len(cols)} colonnes")
    
    logger.info(f"\nTotal de colonnes uniques trouvées: {len(all_columns)}")
    
    # Identifier les batches avec des colonnes différentes
    if len(set(batch_column_counts.values())) > 1:
        logger.warning("⚠️  Nombre de colonnes différent entre les batches!")
        logger.info("Pandas va automatiquement gérer les colonnes manquantes avec NaN")
    
    # Concaténer tous les DataFrames (join='outer' par défaut)
    logger.info("\n--- CONCATÉNATION ---")
    df_combined = pd.concat(dfs, ignore_index=True, sort=False)
    logger.info(f"Total de lignes après concaténation: {len(df_combined):,}")
    logger.info(f"Total de colonnes après concaténation: {len(df_combined.columns)}")
    
    # Supprimer les doublons
    logger.info("\n--- NETTOYAGE DES DOUBLONS ---")
    initial_count = len(df_combined)
    
    # On suppose qu'il y a une colonne 'url' ou 'id' pour identifier les doublons
    # Ajuste selon ta structure de données
    if 'url' in df_combined.columns:
        df_cleaned = df_combined.drop_duplicates(subset=['url'], keep='first')
        logger.info(f"Doublons supprimés basés sur 'url'")
    elif 'id' in df_combined.columns:
        df_cleaned = df_combined.drop_duplicates(subset=['id'], keep='first')
        logger.info(f"Doublons supprimés basés sur 'id'")
    else:
        # Suppression des doublons complets si pas de colonne clé
        df_cleaned = df_combined.drop_duplicates()
        logger.info(f"Doublons complets supprimés")
    
    duplicates_removed = initial_count - len(df_cleaned)
    logger.info(f"Nombre de doublons supprimés: {duplicates_removed:,}")
    logger.info(f"Lignes finales: {len(df_cleaned):,}")
    
    # Statistiques sur les données
    logger.info("\n--- STATISTIQUES DES DONNÉES ---")
    logger.info(f"Nombre de colonnes: {len(df_cleaned.columns)}")
    logger.info(f"Colonnes: {', '.join(df_cleaned.columns[:10])}...")
    
    # Valeurs manquantes
    missing_stats = df_cleaned.isnull().sum()
    if missing_stats.sum() > 0:
        logger.info("\nValeurs manquantes par colonne (top 5):")
        top_missing = missing_stats[missing_stats > 0].sort_values(ascending=False).head(5)
        for col, count in top_missing.items():
            pct = (count / len(df_cleaned)) * 100
            logger.info(f"  {col}: {count:,} ({pct:.1f}%)")
    
    return df_cleaned

def filter_dataframe(df):
    """Filtre les données selon les critères métier"""
    logger.info("\n--- FILTRAGE DES DONNÉES ---")
    initial_rows = len(df)
    
    has_title = 'title' in df.columns
    has_current_location = 'currentLocation' in df.columns
    
    logger.info(f"Colonnes disponibles pour filtrage:")
    logger.info(f"  - title: {has_title}")
    logger.info(f"  - currentLocation: {has_current_location}")
    
    if has_title and has_current_location:
        mask = (
            df['title'].notna() & 
            (df['currentLocation'] != 'non exposé')
        )
        logger.info("📊 Filtre: titre présent ET œuvre exposée")
    elif has_title:
        mask = df['title'].notna()
        logger.info("📊 Filtre: titre présent uniquement")
    elif has_current_location:
        mask = df['currentLocation'] != 'non exposé'
        logger.info("📊 Filtre: œuvre exposée uniquement")
    else:
        logger.warning("⚠ Aucune colonne de filtrage trouvée")
        mask = pd.Series([True] * len(df), index=df.index)
    
    df_filtered = df[mask].copy()
    
    rows_removed = initial_rows - len(df_filtered)
    logger.info(f"Lignes initiales: {initial_rows:,}")
    logger.info(f"Lignes conservées: {len(df_filtered):,}")
    logger.info(f"Lignes supprimées: {rows_removed:,} ({rows_removed/initial_rows*100:.1f}%)")
    
    return df_filtered


def drop_unnecessary_columns(df):
    """Supprime les colonnes non nécessaires"""
    logger.info("\n--- SUPPRESSION DES COLONNES INUTILES ---")
    
    columns_to_drop = [
        'index.Original artwork',
        'titleComplement',
        'index.Imagery',
        'index.Language',
        'objectNumber',
        'printsDrawingsEntity',
        'printsDrawingsCollection',
        'index.Names and titles',
        'index.Script',
        'index.Nature of text',
        'jabachInventory',
        'historicalContext',
        'provenance',
        'shape',
        'onomastics',
        'namesAndTitles',
        'printState',
        'originalObject',
        'index',
        'index.Description/Features',
        'index.objectType',
        'napoleonInventory',
        'bibliography',
        'exhibition',
        'relatedWork',
        'objectType',
        'relatedWork',
        'inscriptions',
        'index.Places',
        'index.Subjects',
        'index.collection',
        'index.People',
        'longTermLoanTo',
        'index.Name',
        'placeOfCreation',
        'dateOfDiscovery',
        'placeOfDiscovery',
        'materialsAndTechniques',
        'index.Materials',
        "index.Mode d'acquisition",
        'modified ',
        'denominationTitle',
        'previousOwner',
        'ownedBy',
        'heldBy',
        'objectHistory',
        'dimension',
        'isMuseesNationauxRecuperation'
    ]
    
    existing_cols_to_drop = [col for col in columns_to_drop if col in df.columns]
    missing_cols = [col for col in columns_to_drop if col not in df.columns]
    
    if missing_cols:
        logger.warning(f"⚠ Colonnes introuvables: {', '.join(missing_cols)}")
    
    if existing_cols_to_drop:
        initial_cols = len(df.columns)
        df = df.drop(columns=existing_cols_to_drop)
        logger.info(f"✓ {len(existing_cols_to_drop)} colonnes supprimées")
        logger.info(f"📊 Colonnes: {initial_cols} → {len(df.columns)}")
    else:
        logger.info("ℹ Aucune colonne à supprimer")
    
    return df

def save_merged_data(df):
    """Sauvegarde le DataFrame mergé"""
    
    if df is None or len(df) == 0:
        logger.error("Aucune donnée à sauvegarder!")
        return None
    
    logger.info("\n--- SAUVEGARDE ---")
    
    # Nom de fichier simple
    filename = "all_works_of_art_le_louvre_merged.csv"
    filepath = OUTPUT_DIR / filename
    
    # Sauvegarder
    try:
        df.to_csv(
            filepath,
            index=False,
            encoding='utf-8-sig',
            escapechar='\\'
        )
        logger.info(f"✓ Fichier sauvegardé: {filepath}")
        logger.info(f"  Taille: {filepath.stat().st_size / (1024*1024):.2f} MB")
        
        return filepath
        
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde: {e}")
        return None

def main():
    """Fonction principale"""
    
    start_time = datetime.now()
    logger.info(f"Début du traitement: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Merger les batches
    df_merged = merge_all_batches()
    
    if df_merged is not None:

        # Filtrer les données
        df_merged = filter_dataframe(df_merged)
        
        # Supprimer les colonnes inutiles
        df_merged = drop_unnecessary_columns(df_merged)

        # Sauvegarder
        output_file = save_merged_data(df_merged)
        
        if output_file:
            # Résumé final
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("\n" + "="*60)
            logger.info("MERGE TERMINÉ AVEC SUCCÈS!")
            logger.info("="*60)
            logger.info(f"Lignes finales: {len(df_merged):,}")
            logger.info(f"Fichier de sortie: {output_file.name}")
            logger.info(f"Durée totale: {duration:.2f} secondes")
            logger.info("="*60)
            
            return df_merged
    
    else:
        logger.error("\n" + "="*60)
        logger.error("ÉCHEC DU MERGE")
        logger.error("="*60)
        return None

if __name__ == "__main__":
    main()