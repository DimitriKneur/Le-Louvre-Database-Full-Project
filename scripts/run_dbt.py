"""
Lance dbt build avec les variables d'environnement du fichier .env
Usage: python run_dbt.py
"""

import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Définir les chemins
script_dir = Path(__file__).parent  # scripts/
project_root = script_dir.parent    # Le-Louvre-Database-Full_Project/
env_path = project_root / '.env'    # Le-Louvre-Database-Full_Project/.env
dbt_dir = project_root / 'dbt_louvre'  # Le-Louvre-Database-Full_Project/dbt_louvre/

# Charger les variables d'environnement depuis le fichier .env
if env_path.exists():
    load_dotenv(env_path)
    print(f"✓ Variables d'environnement chargées depuis {env_path}")
else:
    print(f"❌ Fichier .env non trouvé à {env_path}")
    sys.exit(1)

# Afficher les variables chargées (sans le mot de passe)
print(f"  SUPABASE_HOST: {os.getenv('SUPABASE_HOST')}")
print(f"  SUPABASE_USER: {os.getenv('SUPABASE_USER')}")
print(f"  SUPABASE_PORT: {os.getenv('SUPABASE_PORT')}")
print(f"  SUPABASE_DATABASE: {os.getenv('SUPABASE_DATABASE')}")
print()

# Lancer dbt build
dbt_command = ['dbt', 'build']

print(f"Exécution: {' '.join(dbt_command)}")
print(f"Dossier de travail: {dbt_dir}")
print("=" * 60)
print()

try:
    result = subprocess.run(
        dbt_command,
        env=os.environ.copy(),
        cwd=dbt_dir
    )
    sys.exit(result.returncode)
    
except FileNotFoundError:
    print("\n❌ Erreur: dbt n'est pas installé ou n'est pas dans le PATH")
    print("Installez dbt avec: pip install dbt-postgres")
    sys.exit(1)
    
except KeyboardInterrupt:
    print("\n\n⚠ Commande interrompue par l'utilisateur")
    sys.exit(130)