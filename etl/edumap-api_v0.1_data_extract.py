import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
import os
import sys
import traceback
import time

# Configuration du logging avec affichage console en plus du fichier
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('import_excel_normalized.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Configuration de la connexion à la base de données MySQL
config = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'edumap-api_v0.1',
    'raise_on_warnings': True
}

def verify_database_tables(connexion):
    """Vérifie que toutes les tables existent et récupère leur structure"""
    required_tables = [
        'localisations', 'milieux', 'statuts', 'systemes', 'annees', 
        'etablissements', 'equipements_etablissement', 'effectifs', 'infrastructures'
    ]
    
    try:
        cursor = connexion.cursor()
        
        for table in required_tables:
            cursor.execute(f"SHOW TABLES LIKE '{table}'")
            table_exists = cursor.fetchone()
            
            if not table_exists:
                logging.error(f"La table '{table}' n'existe pas dans la base de données.")
                return False
            
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            logging.info(f"Structure de la table {table}: {[col[0] for col in columns]}")
        
        return True
        
    except Error as e:
        logging.error(f"Erreur lors de la vérification des tables: {e}")
        return False
    finally:
        cursor.close()

def verify_excel_file(file_path):
    """Vérifie que le fichier Excel existe et est accessible"""
    if not os.path.exists(file_path):
        logging.error(f"Le fichier {file_path} n'existe pas.")
        return False
    return True

def print_dataframe_sample(df):
    """Affiche un échantillon du DataFrame pour débogage"""
    sample = df.head(3)
    logging.debug(f"Échantillon du DataFrame:\n{sample}")
    logging.debug(f"Types de données:\n{df.dtypes}")
    
    missing = df.isnull().sum()
    if missing.sum() > 0:
        logging.debug(f"Valeurs manquantes par colonne:\n{missing[missing > 0]}")

def clean_data(df):
    """Nettoie et prépare les données"""
    logging.info("Début du nettoyage des données...")
    
    # Liste des colonnes numériques
    colonnes_numeriques = [
        'sommedenb_eff_g', 'sommedenb_eff_f', 'Tot', 'sommedenb_ens_h', 
        'sommedenb_ens_f', 'Total ense', 'sommedenb_salles_classes_dur', 
        'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'
    ]
    
    # Remplacer les valeurs NaN
    existing_num_cols = [col for col in colonnes_numeriques if col in df.columns]
    df[existing_num_cols] = df[existing_num_cols].fillna(0)
    df = df.fillna('')
    
    # Nettoyer les coordonnées géographiques
    if 'LATITUDE' in df.columns:
        try:
            df['LATITUDE'] = df['LATITUDE'].astype(str).str.replace(',', '.')
            df['LATITUDE'] = df['LATITUDE'].replace('', '0').replace('nan', '0')
            df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce').fillna(0)
            df['LATITUDE'] = df['LATITUDE'].apply(lambda x: x if -90 <= x <= 90 else 0)
        except Exception as e:
            logging.error(f"Erreur lors du nettoyage des latitudes: {e}")
    
    if 'LONGITUDE' in df.columns:
        try:
            df['LONGITUDE'] = df['LONGITUDE'].astype(str).str.replace(',', '.')
            df['LONGITUDE'] = df['LONGITUDE'].replace('', '0').replace('nan', '0')
            df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce').fillna(0)
            df['LONGITUDE'] = df['LONGITUDE'].apply(lambda x: x if -180 <= x <= 180 else 0)
        except Exception as e:
            logging.error(f"Erreur lors du nettoyage des longitudes: {e}")
    
    logging.info("Nettoyage des données terminé")
    return df

def get_or_create_lookup_id(cursor, table, column, value):
    """Récupère ou crée un ID dans une table de lookup"""
    if not value or str(value).strip() == '':
        return None
    
    value_str = str(value).strip()
    
    # Chercher si la valeur existe déjà
    cursor.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value_str,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    
    # Insérer la nouvelle valeur
    cursor.execute(f"INSERT INTO {table} ({column}) VALUES (%s)", (value_str,))
    return cursor.lastrowid

def insert_localisation(cursor, row):
    """Insère une localisation et retourne son ID"""
    # Vérifier si la localisation existe déjà
    cursor.execute("""
        SELECT id FROM localisations 
        WHERE region = %s AND prefecture = %s AND canton_village_autonome = %s 
        AND ville_village_quartier = %s AND commune_etab = %s
    """, (
        str(row.get('region', '')),
        str(row.get('prefecture', '')),
        str(row.get('canton_village_autonome', '')),
        str(row.get('ville_village_quartier', '')),
        str(row.get('commune_etab', ''))
    ))
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Insérer la nouvelle localisation
    cursor.execute("""
        INSERT INTO localisations (region, prefecture, canton_village_autonome, 
                                 ville_village_quartier, commune_etab, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        str(row.get('region', '')),
        str(row.get('prefecture', '')),
        str(row.get('canton_village_autonome', '')),
        str(row.get('ville_village_quartier', '')),
        str(row.get('commune_etab', '')),
        float(row.get('LATITUDE', 0)),
        float(row.get('LONGITUDE', 0))
    ))
    
    return cursor.lastrowid

def insert_etablissement_data(connexion, df, batch_size=100):
    """Insère les données des établissements dans la structure normalisée"""
    try:
        cursor = connexion.cursor()
        total_rows = len(df)
        batches = (total_rows // batch_size) + (1 if total_rows % batch_size > 0 else 0)
        
        logging.info(f"Début de l'insertion des données ({total_rows} lignes, {batches} lots)")
        start_time = time.time()
        successful_inserts = 0
        
        for i in range(batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_rows)
            batch = df.iloc[start_idx:end_idx]
            
            try:
                connexion.start_transaction()
                
                for idx, row in batch.iterrows():
                    try:
                        # 1. Insérer/récupérer les IDs des tables de référence
                        milieu_id = get_or_create_lookup_id(
                            cursor, 'milieux', 'libelle_type_milieu', 
                            row.get('libelle_type_milieu')
                        )
                        
                        statut_id = get_or_create_lookup_id(
                            cursor, 'statuts', 'libelle_type_statut_etab', 
                            row.get('libelle_type_statut_etab')
                        )
                        
                        systeme_id = get_or_create_lookup_id(
                            cursor, 'systemes', 'libelle_type_systeme', 
                            row.get('libelle_type_systeme')
                        )
                        
                        annee_id = get_or_create_lookup_id(
                            cursor, 'annees', 'libelle_type_annee', 
                            row.get('libelle_type_annee')
                        )
                        
                        # 2. Insérer la localisation
                        localisation_id = insert_localisation(cursor, row)
                        
                        # 3. Insérer l'établissement
                        cursor.execute("""
                            INSERT INTO etablissements (code_etablissement, nom_etablissement,
                                                       localisation_id, milieu_id, statut_id, 
                                                       systeme_id, annee_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            str(row.get('code_etablissement', '')),
                            str(row.get('nom_etablissement', '')),
                            localisation_id,
                            milieu_id,
                            statut_id,
                            systeme_id,
                            annee_id
                        ))
                        
                        etablissement_id = cursor.lastrowid
                        
                        # 4. Insérer les équipements
                        cursor.execute("""
                            INSERT INTO equipements_etablissement (etablissement_id, existe_elect,
                                                                  existe_latrine, existe_latrine_fonct,
                                                                  acces_toute_saison, eau)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            etablissement_id,
                            str(row.get('existe_elect', 'false')).lower() == 'true',
                            str(row.get('existe_latrine', 'false')).lower() == 'true',
                            str(row.get('existe_latrine_fonct', 'false')).lower() == 'true',
                            str(row.get('acces_toute_saison', 'false')).lower() == 'true',
                            str(row.get('eau', 'false')).lower() == 'true'
                        ))
                        
                        # 5. Insérer les effectifs
                        cursor.execute("""
                            INSERT INTO effectifs (etablissement_id, sommedenb_eff_g, sommedenb_eff_f,
                                                 tot, sommedenb_ens_h, sommedenb_ens_f, total_ense)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            etablissement_id,
                            int(float(row.get('sommedenb_eff_g', 0))),
                            int(float(row.get('sommedenb_eff_f', 0))),
                            int(float(row.get('Tot', 0))),
                            int(float(row.get('sommedenb_ens_h', 0))),
                            int(float(row.get('sommedenb_ens_f', 0))),
                            int(float(row.get('Total ense', 0)))
                        ))
                        
                        # 6. Insérer les infrastructures
                        cursor.execute("""
                            INSERT INTO infrastructures (etablissement_id, sommedenb_salles_classes_dur,
                                                       sommedenb_salles_classes_banco, sommedenb_salles_classes_autre)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            etablissement_id,
                            int(float(row.get('sommedenb_salles_classes_dur', 0))),
                            int(float(row.get('sommedenb_salles_classes_banco', 0))),
                            int(float(row.get('sommedenb_salles_classes_autre', 0)))
                        ))
                        
                        successful_inserts += 1
                        
                    except Exception as e:
                        logging.error(f"Erreur avec la ligne {idx}: {e}")
                        continue
                
                connexion.commit()
                batch_time = time.time() - start_time
                percent_complete = (end_idx / total_rows) * 100
                logging.info(f"Lot {i+1}/{batches} : {len(batch)} lignes traitées en {batch_time:.2f}s ({percent_complete:.1f}% terminé)")
                
            except Error as e:
                connexion.rollback()
                logging.error(f"Erreur lors de l'insertion du lot {i+1}: {e}")
                continue
        
        total_time = time.time() - start_time
        logging.info(f"Insertion terminée en {total_time:.2f} secondes.")
        logging.info(f"Lignes insérées avec succès: {successful_inserts}/{total_rows}")
        
        return successful_inserts > 0
        
    except Error as e:
        connexion.rollback()
        logging.error(f"Erreur lors de l'insertion des données: {e}")
        return False
    finally:
        cursor.close()

def test_database_connection():
    """Teste la connexion à la base de données"""
    try:
        connexion = mysql.connector.connect(**config)
        if connexion.is_connected():
            db_info = connexion.get_server_info()
            logging.info(f"Connecté au serveur MySQL version {db_info}")
            
            cursor = connexion.cursor()
            cursor.execute("SELECT DATABASE();")
            db_name = cursor.fetchone()[0]
            logging.info(f"Base de données actuelle: {db_name}")
            
            cursor.close()
            connexion.close()
            logging.info("Test de connexion réussi")
            return True
        else:
            logging.error("Échec de connexion à la base de données")
            return False
    except Error as e:
        logging.error(f"Erreur lors du test de connexion à MySQL: {e}")
        return False

def main():
    # Chemin du fichier Excel
    fichier_excel = 'C:/Users/_Salim_mevtr_/project-lab/edumap-api/elt/Base_2024.xlsx'
    
    # Test de la connexion à la base de données
    logging.info("Test de la connexion à la base de données...")
    if not test_database_connection():
        logging.error("Impossible de se connecter à la base de données. Vérifiez les paramètres de connexion.")
        return
    
    # Vérifier l'existence du fichier
    if not verify_excel_file(fichier_excel):
        logging.error(f"Le fichier {fichier_excel} n'existe pas.")
        return
    
    try:
        # Charger le fichier Excel
        logging.info(f"Chargement du fichier {fichier_excel}")
        df = pd.read_excel(fichier_excel)
        logging.info(f"Fichier chargé avec succès. {len(df)} lignes trouvées.")
        
        # Afficher un échantillon pour débogage
        print_dataframe_sample(df)
        
        # Nettoyer les données
        df = clean_data(df)
        
        # Établir une connexion à la base de données
        logging.info("Connexion à la base de données...")
        connexion = mysql.connector.connect(**config)
        
        if connexion.is_connected():
            logging.info("Connexion établie avec succès!")
            
            # Vérifier la structure des tables
            if not verify_database_tables(connexion):
                logging.error("Problème avec la structure des tables.")
                return
            
            # Insérer les données
            if insert_etablissement_data(connexion, df):
                logging.info("Toutes les données ont été insérées avec succès!")
            else:
                logging.error("L'insertion des données a échoué.")
                
    except pd.errors.EmptyDataError:
        logging.error("Le fichier Excel est vide.")
    except pd.errors.ParserError:
        logging.error("Erreur lors de l'analyse du fichier Excel.")
    except Error as e:
        logging.error(f"Erreur MySQL: {e}")
    except Exception as e:
        logging.error(f"Erreur inattendue: {e}")
        logging.error(traceback.format_exc())
    finally:
        # Fermer la connexion
        if 'connexion' in locals() and connexion.is_connected():
            connexion.close()
            logging.info("Connexion à la base de données fermée.")

if __name__ == "__main__":
    main()