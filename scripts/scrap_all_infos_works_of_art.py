import aiohttp
import os
import asyncio
import pandas as pd
import json
import nest_asyncio
from pathlib import Path
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

nest_asyncio.apply()

# Configuration
BATCH_SIZE = 5000  # Réduit à 5000 pour plus de stabilité
MAX_CONCURRENT_REQUESTS = 50  # Limite les requêtes simultanées
REQUEST_TIMEOUT = 30  # Timeout en secondes

script_dir = Path(__file__).parent
# Remonter au dossier parent (racine du projet)
project_root = script_dir.parent

# Dossier de sortie pour les batches
OUTPUT_DIR = project_root / "data" / "output_batches"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # parents=True crée les dossiers parents si nécessaire

# Chemin vers le fichier JSON d'input
json_path = project_root / "data" / "urls_le_louvre_all.json"

# Lire le fichier JSON
df_urls = pd.read_json(json_path)
logger.info(f"Nombre total d'URLs à scraper : {len(df_urls)}")

# Function to fetch data from a single URL
async def fetch(session, url, semaphore):
    async with semaphore:  # Limite le nombre de requêtes concurrentes
        try:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            async with session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    logger.warning(f"Non-200 response for URL: {url} with status: {response.status}")
                    return None
                if 'application/json' not in response.headers.get('Content-Type', ''):
                    logger.warning(f"Non-JSON response for URL: {url}")
                    return None
                return await response.json()
        except asyncio.TimeoutError:
            logger.error(f"Timeout for URL: {url}")
            return None
        except aiohttp.ClientError as e:
            logger.error(f"Request failed for URL: {url} - {type(e).__name__}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for URL: {url} - {type(e).__name__}: {e}")
            return None

# Function to handle retries with exponential backoff
async def fetch_with_retry(session, url, semaphore, retries=3):
    for attempt in range(retries):
        result = await fetch(session, url, semaphore)
        if result is not None:
            return result
        if attempt < retries - 1:
            wait_time = 2 ** attempt
            logger.debug(f"Retry {attempt + 1}/{retries} for {url} after {wait_time}s")
            await asyncio.sleep(wait_time)
    logger.error(f"Failed to fetch URL after {retries} retries: {url}")
    return None

# Function to fetch data from all URLs with controlled concurrency
async def fetch_batch(urls, batch_number):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            full_url = f"https://collections.louvre.fr{url}.json"
            tasks.append(fetch_with_retry(session, full_url, semaphore))
        
        logger.info(f"Batch {batch_number}: Fetching {len(tasks)} URLs...")
        results = await asyncio.gather(*tasks)
        
        # Statistiques
        successful = sum(1 for r in results if r is not None)
        failed = len(results) - successful
        logger.info(f"Batch {batch_number}: {successful} successful, {failed} failed")
        
        return results

# Fonction pour sauvegarder un batch
def save_batch(data, batch_number):
    if not data:
        logger.warning(f"Batch {batch_number}: No data to save")
        return None
    
    # Filtrer les None
    filtered_data = [d for d in data if d is not None]
    
    if not filtered_data:
        logger.warning(f"Batch {batch_number}: All requests failed")
        return None
    
    # Normaliser en DataFrame
    df = pd.json_normalize(filtered_data)
    
    # Sauvegarder le batch
    batch_file = OUTPUT_DIR / f"batch_{batch_number:04d}.csv"
    df.to_csv(batch_file, index=False, encoding='utf-8-sig', escapechar='\\')
    logger.info(f"Batch {batch_number}: Saved {len(df)} rows to {batch_file}")
    
    return df

# Fonction principale pour traiter tous les batches
async def process_all_batches():
    total_urls = len(df_urls)
    num_batches = (total_urls + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"Processing {total_urls} URLs in {num_batches} batches of {BATCH_SIZE}")
    
    all_dataframes = []
    
    for i in range(num_batches):
        start_idx = i * BATCH_SIZE
        end_idx = min((i + 1) * BATCH_SIZE, total_urls)
        batch_number = i + 1
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Starting batch {batch_number}/{num_batches} (URLs {start_idx} to {end_idx})")
        logger.info(f"{'='*60}")
        
        # Extraire les URLs du batch
        batch_urls = df_urls['url'][start_idx:end_idx]
        
        # Fetch les données
        data = await fetch_batch(batch_urls, batch_number)
        
        # Sauvegarder le batch
        df_batch = save_batch(data, batch_number)
        
        if df_batch is not None:
            all_dataframes.append(df_batch)
        
        # Pause entre les batches pour éviter de surcharger le serveur
        if i < num_batches - 1:
            logger.info("Pausing 5 seconds before next batch...")
            await asyncio.sleep(5)
    
    return all_dataframes

# Fonction pour fusionner tous les batches
def merge_all_batches():
    logger.info("\n" + "="*60)
    logger.info("Merging all batches...")
    logger.info("="*60)
    
    # Lire tous les fichiers CSV du dossier output_data
    batch_files = sorted(OUTPUT_DIR.glob("batch_*.csv"))
    
    if not batch_files:
        logger.error("No batch files found!")
        return None
    
    logger.info(f"Found {len(batch_files)} batch files")
    
    # Lire et concaténer tous les batches
    dfs = []
    for batch_file in batch_files:
        df = pd.read_csv(batch_file, encoding='utf-8-sig')
        dfs.append(df)
        logger.info(f"Loaded {batch_file.name}: {len(df)} rows")
    
    # Concaténer
    df_final = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total rows after concatenation: {len(df_final)}")
    
    # Supprimer les doublons
    df_final_clean = df_final.drop_duplicates()
    duplicates_removed = len(df_final) - len(df_final_clean)
    logger.info(f"Duplicates removed: {duplicates_removed}")
    
    # Sauvegarder le fichier final
    final_file = "all_works_of_art_le_louvre_uncleaned.csv"
    df_final_clean.to_csv(
        final_file, 
        index=False, 
        encoding='utf-8-sig',  # Pour gérer les caractères spéciaux
        escapechar='\\'
    )
    logger.info(f"Final file saved: {final_file} with {len(df_final_clean)} rows")
    
    return df_final_clean

# Fonction pour reprendre à partir d'un batch spécifique (si plantage)
async def resume_from_batch(start_batch):
    total_urls = len(df_urls)
    num_batches = (total_urls + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"Resuming from batch {start_batch}/{num_batches}")
    
    for i in range(start_batch - 1, num_batches):
        start_idx = i * BATCH_SIZE
        end_idx = min((i + 1) * BATCH_SIZE, total_urls)
        batch_number = i + 1
        
        # Vérifier si le batch existe déjà
        batch_file = OUTPUT_DIR / f"batch_{batch_number:04d}.csv"
        if batch_file.exists():
            logger.info(f"Batch {batch_number} already exists, skipping...")
            continue
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing batch {batch_number}/{num_batches} (URLs {start_idx} to {end_idx})")
        logger.info(f"{'='*60}")
        
        batch_urls = df_urls['url'][start_idx:end_idx]
        data = await fetch_batch(batch_urls, batch_number)
        save_batch(data, batch_number)
        
        if i < num_batches - 1:
            await asyncio.sleep(5)

# EXÉCUTION PRINCIPALE
if __name__ == "__main__":
    try:
        # Option 1 : Tout scraper depuis le début
        logger.info("Starting full scraping process...")
        asyncio.run(process_all_batches())
        
        # Option 2 : Reprendre à partir du batch 10 (si plantage)
        # asyncio.run(resume_from_batch(10))
        
        # Fusionner tous les batches
        df_final = merge_all_batches()
        
        logger.info("\n" + "="*60)
        logger.info("SCRAPING COMPLETED SUCCESSFULLY!")
        logger.info(f"Final dataset shape: {df_final.shape}")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Fatal error: {type(e).__name__}: {e}", exc_info=True)