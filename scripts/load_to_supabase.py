import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import logging
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration Supabase depuis .env
DB_CONFIG = {
    'host': os.getenv('SUPABASE_HOST'),
    'user': os.getenv('SUPABASE_USER'),
    'password': os.getenv('SUPABASE_PASSWORD'),
    'port': os.getenv('SUPABASE_PORT', 5432),
    'database': os.getenv('SUPABASE_DATABASE'),
    'schema': 'source_raw'
}

# Chemins
script_dir = Path(__file__).parent
project_root = script_dir.parent
DATA_DIR = project_root / "data"
CSV_FILE = DATA_DIR / "merged_output" / "all_works_of_art_le_louvre_merged.csv"

# Nom de la table dans Supabase
TABLE_NAME = "all_works_of_art_le_louvre_raw"


def create_connection():
    """Crée une connexion à la base de données Supabase"""
    import psycopg  # psycopg3
    
    logger.info(f"DEBUG - Using: psycopg3")
    logger.info(f"DEBUG - Host: {DB_CONFIG['host']}")
    logger.info(f"DEBUG - User: {DB_CONFIG['user']}")
    logger.info(f"DEBUG - Port: {DB_CONFIG['port']}")
    logger.info(f"DEBUG - Database: {DB_CONFIG['database']}")
    
    try:
        logger.info("Test de connexion directe psycopg3...")
        
        # Construction de la connection string
        conninfo = (
            f"host={DB_CONFIG['host']} "
            f"port={DB_CONFIG['port']} "
            f"dbname={DB_CONFIG['database']} "
            f"user={DB_CONFIG['user']} "
            f"password={DB_CONFIG['password']}"
        )
        
        test_conn = psycopg.connect(conninfo)
        test_conn.close()
        logger.info("✓ Test connexion psycopg3 réussi !")
        
    except Exception as e:
        logger.error(f"✗ Erreur connexion: {type(e).__name__}: {e}")
        raise
    
    # Créer l'engine SQLAlchemy avec psycopg (version 3)
    connection_string = (
        f"postgresql+psycopg://"
        f"{quote_plus(DB_CONFIG['user'])}:"
        f"{quote_plus(DB_CONFIG['password'])}@"
        f"{DB_CONFIG['host']}:"
        f"{DB_CONFIG['port']}/"
        f"{DB_CONFIG['database']}"
    )
    
    engine = create_engine(
        connection_string,
        pool_pre_ping=True
    )
    
    logger.info("✓ Connexion à Supabase établie")
    return engine


def load_csv_data():
    """Charge le fichier CSV"""
    logger.info(f"Chargement du fichier: {CSV_FILE}")
    
    if not CSV_FILE.exists():
        logger.error(f"Fichier non trouvé: {CSV_FILE}")
        return None
    
    df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
    logger.info(f"✓ Données chargées: {len(df):,} lignes, {len(df.columns)} colonnes")
    
    return df


def prepare_dataframe(df):
    """Prépare le DataFrame pour l'insertion"""
    logger.info("Préparation des données...")
    
    # Convertir toutes les colonnes en string (varchar)
    for col in df.columns:
        df[col] = df[col].astype(str)
        # Remplacer 'nan' par None pour les vraies valeurs NULL
        df[col] = df[col].replace('nan', None)
    
    # Nettoyer les noms de colonnes pour PostgreSQL
    df.columns = [col.lower().replace(' ', '_').replace('.', '_') for col in df.columns]
    
    logger.info(f"✓ Colonnes nettoyées: {', '.join(df.columns[:5])}...")
    
    return df


def create_table_if_not_exists(engine, df):
    """Crée la table si elle n'existe pas"""
    logger.info(f"Vérification/création de la table: {TABLE_NAME}")
    
    # Générer la définition de la table (toutes les colonnes en TEXT)
    columns_def = []
    for col in df.columns:
        columns_def.append(f'"{col}" TEXT')  # TEXT au lieu de VARCHAR
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {DB_CONFIG['schema']}.{TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        {', '.join(columns_def)},
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    
    logger.info(f"✓ Table {TABLE_NAME} prête")


def truncate_table(engine):
    """Vide la table avant insertion"""
    logger.info(f"Vidage de la table {TABLE_NAME}...")
    
    truncate_sql = f"TRUNCATE TABLE {DB_CONFIG['schema']}.{TABLE_NAME} RESTART IDENTITY;"
    
    with engine.connect() as conn:
        conn.execute(text(truncate_sql))
        conn.commit()
    
    logger.info("✓ Table vidée")


def load_data_to_supabase(df, engine, batch_size=1000):
    """Charge les données dans Supabase par batch"""
    logger.info(f"Chargement des données par batch de {batch_size}...")
    
    total_rows = len(df)
    num_batches = (total_rows + batch_size - 1) // batch_size
    
    for i in range(0, total_rows, batch_size):
        batch_num = (i // batch_size) + 1
        df_batch = df.iloc[i:i+batch_size]
        
        try:
            df_batch.to_sql(
                TABLE_NAME,
                engine,
                schema=DB_CONFIG['schema'],
                if_exists='append',
                index=False,
                method='multi'
            )
            logger.info(f"✓ Batch {batch_num}/{num_batches}: {len(df_batch)} lignes insérées")
        except Exception as e:
            logger.error(f"✗ Erreur batch {batch_num}: {e}")
            raise
    
    logger.info(f"✓ Toutes les données chargées: {total_rows:,} lignes")


def verify_data(engine):
    """Vérifie que les données sont bien chargées"""
    logger.info("Vérification des données...")
    
    query = f"SELECT COUNT(*) as count FROM {DB_CONFIG['schema']}.{TABLE_NAME};"
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        count = result.fetchone()[0]
    
    logger.info(f"✓ Nombre de lignes dans la table: {count:,}")
    return count


def main():
    """Fonction principale"""
    try:
        logger.info("="*60)
        logger.info("CHARGEMENT DES DONNÉES DANS SUPABASE")
        logger.info("="*60)
        
        # 1. Charger le CSV
        df = load_csv_data()
        if df is None:
            return
        
        # 2. Préparer les données
        df = prepare_dataframe(df)
        
        # 3. Créer connexion
        engine = create_connection()
        
        # 4. Créer la table si nécessaire
        create_table_if_not_exists(engine, df)
        
        # 5. Vider la table (fresh start)
        truncate_table(engine)
        
        # 6. Charger les données
        load_data_to_supabase(df, engine, batch_size=1000)
        
        # 7. Vérifier
        count = verify_data(engine)
        
        logger.info("\n" + "="*60)
        logger.info("CHARGEMENT TERMINÉ AVEC SUCCÈS!")
        logger.info(f"Table: {TABLE_NAME}")
        logger.info(f"Lignes insérées: {count:,}")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"\n{'='*60}")
        logger.error("ERREUR LORS DU CHARGEMENT")
        logger.error(f"{'='*60}")
        logger.error(f"{type(e).__name__}: {e}", exc_info=True)


if __name__ == "__main__":
    main()