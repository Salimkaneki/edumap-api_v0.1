import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
import os
import sys
import traceback
import time
from decimal import Decimal, ROUND_HALF_UP

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
    'database': 'edumap_api_v01',
    'raise_on_warnings': True
}

def map_columns(df):
    """Mappe les noms de colonnes pour assurer la cohérence"""
    # Mapping des colonnes avec différentes variantes possibles
    column_mapping = {
        # Coordonnées géographiques
        'LATITUDE': 'latitude',
        'Latitude': 'latitude', 
        'LONGITUDE': 'longitude',
        'Longitude': 'longitude',
        
        # Informations établissement
        'CODE_ETABLISSEMENT': 'code_etablissement',
        'Code_etablissement': 'code_etablissement',
        'NOM_ETABLISSEMENT': 'nom_etablissement',
        'Nom_etablissement': 'nom_etablissement',
        
        # Localisation
        'REGION': 'region',
        'Region': 'region',
        'PREFECTURE': 'prefecture',
        'Prefecture': 'prefecture',
        'CANTON_VILLAGE_AUTONOME': 'canton_village_autonome',
        'Canton_village_autonome': 'canton_village_autonome',
        'VILLE_VILLAGE_QUARTIER': 'ville_village_quartier',
        'Ville_village_quartier': 'ville_village_quartier',
        'COMMUNE_ETAB': 'commune_etab',
        'Commune_etab': 'commune_etab',
        
        # Types
        'LIBELLE_TYPE_MILIEU': 'libelle_type_milieu',
        'Libelle_type_milieu': 'libelle_type_milieu',
        'LIBELLE_TYPE_STATUT_ETAB': 'libelle_type_statut_etab',
        'Libelle_type_statut_etab': 'libelle_type_statut_etab',
        'LIBELLE_TYPE_SYSTEME': 'libelle_type_systeme',
        'Libelle_type_systeme': 'libelle_type_systeme',
        'LIBELLE_TYPE_ANNEE': 'libelle_type_annee',
        'Libelle_type_annee': 'libelle_type_annee',
        
        # Equipements
        'EXISTE_ELECT': 'existe_elect',
        'Existe_elect': 'existe_elect',
        'EXISTE_LATRINE': 'existe_latrine',
        'Existe_latrine': 'existe_latrine',
        'EXISTE_LATRINE_FONCT': 'existe_latrine_fonct',
        'Existe_latrine_fonct': 'existe_latrine_fonct',
        'ACCES_TOUTE_SAISON': 'acces_toute_saison',
        'Acces_toute_saison': 'acces_toute_saison',
        'EAU': 'eau',
        'Eau': 'eau',
        
        # Effectifs - CORRECTION: noms conformes à la migration
        'SOMMEDENB_EFF_G': 'sommedenb_eff_g',
        'Sommedenb_eff_g': 'sommedenb_eff_g',
        'SOMMEDENB_EFF_F': 'sommedenb_eff_f',
        'Sommedenb_eff_f': 'sommedenb_eff_f',
        'TOT': 'tot',  # CORRIGÉ: 'tot' au lieu de 'Tot'
        'TOTAL': 'tot',
        'Total': 'tot',
        'SOMMEDENB_ENS_H': 'sommedenb_ens_h',
        'Sommedenb_ens_h': 'sommedenb_ens_h',
        'SOMMEDENB_ENS_F': 'sommedenb_ens_f',
        'Sommedenb_ens_f': 'sommedenb_ens_f',
        'TOTAL_ENSE': 'total_ense',  # CORRIGÉ: 'total_ense' au lieu de 'Total ense'
        'Total_ense': 'total_ense',
        'TOTAL ENSE': 'total_ense',
        
        # Infrastructures
        'SOMMEDENB_SALLES_CLASSES_DUR': 'sommedenb_salles_classes_dur',
        'Sommedenb_salles_classes_dur': 'sommedenb_salles_classes_dur',
        'SOMMEDENB_SALLES_CLASSES_BANCO': 'sommedenb_salles_classes_banco',
        'Sommedenb_salles_classes_banco': 'sommedenb_salles_classes_banco',
        'SOMMEDENB_SALLES_CLASSES_AUTRE': 'sommedenb_salles_classes_autre',
        'Sommedenb_salles_classes_autre': 'sommedenb_salles_classes_autre',
    }
    
    # Appliquer le mapping
    df_mapped = df.rename(columns=column_mapping)
    
    # Afficher les colonnes disponibles pour débogage
    logging.info(f"Colonnes disponibles après mapping: {list(df_mapped.columns)}")
    
    return df_mapped

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
    
    # Liste des colonnes numériques - CORRIGÉE selon la migration
    colonnes_numeriques = [
        'sommedenb_eff_g', 'sommedenb_eff_f', 'tot', 'sommedenb_ens_h', 
        'sommedenb_ens_f', 'total_ense', 'sommedenb_salles_classes_dur', 
        'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'
    ]
    
    # Remplacer les valeurs NaN
    existing_num_cols = [col for col in colonnes_numeriques if col in df.columns]
    df[existing_num_cols] = df[existing_num_cols].fillna(0)
    df = df.fillna('')
    
    # Nettoyer les coordonnées géographiques avec précision Decimal
    if 'latitude' in df.columns:
        try:
            df['latitude'] = df['latitude'].astype(str).str.replace(',', '.')
            df['latitude'] = df['latitude'].replace('', '0').replace('nan', '0')
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce').fillna(0)
            df['latitude'] = df['latitude'].apply(lambda x: x if -90 <= x <= 90 else None)
        except Exception as e:
            logging.error(f"Erreur lors du nettoyage des latitudes: {e}")
    
    if 'longitude' in df.columns:
        try:
            df['longitude'] = df['longitude'].astype(str).str.replace(',', '.')
            df['longitude'] = df['longitude'].replace('', '0').replace('nan', '0')
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce').fillna(0)
            df['longitude'] = df['longitude'].apply(lambda x: x if -180 <= x <= 180 else None)
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
    """Insère une localisation et retourne son ID (SANS coordonnées comme spécifié dans la migration)"""
    # Vérifier si la localisation existe déjà
    cursor.execute("""
        SELECT id FROM localisations 
        WHERE region = %s AND prefecture = %s AND canton_village_autonome = %s 
        AND ville_village_quartier = %s AND COALESCE(commune_etab, '') = %s
    """, (
        str(row.get('region', '')),
        str(row.get('prefecture', '')),
        str(row.get('canton_village_autonome', '')),
        str(row.get('ville_village_quartier', '')),
        str(row.get('commune_etab', '')) if row.get('commune_etab') else ''
    ))
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Insérer la nouvelle localisation SANS coordonnées (conformément à la migration)
    commune_etab = str(row.get('commune_etab', '')) if row.get('commune_etab') else None
    
    cursor.execute("""
        INSERT INTO localisations (region, prefecture, canton_village_autonome, 
                                 ville_village_quartier, commune_etab)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        str(row.get('region', '')),
        str(row.get('prefecture', '')),
        str(row.get('canton_village_autonome', '')),
        str(row.get('ville_village_quartier', '')),
        commune_etab
    ))
    
    return cursor.lastrowid

def convert_to_decimal(value, default=None, precision=8):
    """
    Convertit une valeur en Decimal avec la précision correcte pour MySQL
    
    Args:
        value: La valeur à convertir
        default: Valeur par défaut si conversion impossible
        precision: Nombre de décimales (8 pour latitude/longitude selon migration)
    
    Returns:
        Decimal formaté ou None
    """
    if value is None or value == '' or str(value).strip() == '':
        return default
    
    try:
        # Convertir en float d'abord
        num_value = float(value)
        
        # Si la valeur est 0, retourner default
        if num_value == 0:
            return default
        
        # Créer un Decimal et le formater avec la précision correcte
        # Pour latitude: DECIMAL(10,8) = 2 chiffres avant la virgule, 8 après
        # Pour longitude: DECIMAL(11,8) = 3 chiffres avant la virgule, 8 après
        decimal_value = Decimal(str(num_value))
        
        # Formater avec la précision requise
        quantized = decimal_value.quantize(
            Decimal('0.' + '0' * precision), 
            rounding=ROUND_HALF_UP
        )
        
        return quantized
        
    except (ValueError, TypeError, Exception) as e:
        logging.warning(f"Impossible de convertir '{value}' en Decimal: {e}")
        return default

def convert_to_boolean(value):
    """Convertit une valeur en booléen de manière sûre"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ['true', '1', 'yes', 'oui', 'vrai']
    if isinstance(value, (int, float)):
        return bool(value)
    return False

def check_etablissement_exists(cursor, code_etablissement):
    """Vérifie si un établissement existe déjà par son code"""
    cursor.execute("SELECT id FROM etablissements WHERE code_etablissement = %s", (code_etablissement,))
    result = cursor.fetchone()
    return result[0] if result else None

def validate_coordinates(latitude, longitude):
    """
    Valide les coordonnées géographiques
    
    Args:
        latitude: Valeur de latitude
        longitude: Valeur de longitude
    
    Returns:
        tuple: (latitude_valid, longitude_valid) avec valeurs corrigées ou None
    """
    valid_lat = None
    valid_lng = None
    
    if latitude is not None:
        try:
            lat_float = float(latitude)
            if -90 <= lat_float <= 90:
                valid_lat = convert_to_decimal(lat_float, precision=8)
            else:
                logging.warning(f"Latitude hors limites: {lat_float}")
        except (ValueError, TypeError):
            logging.warning(f"Latitude non valide: {latitude}")
    
    if longitude is not None:
        try:
            lng_float = float(longitude)
            if -180 <= lng_float <= 180:
                valid_lng = convert_to_decimal(lng_float, precision=8)
            else:
                logging.warning(f"Longitude hors limites: {lng_float}")
        except (ValueError, TypeError):
            logging.warning(f"Longitude non valide: {longitude}")
    
    return valid_lat, valid_lng

def insert_etablissement_data(connexion, df, batch_size=100):
    """Insère les données des établissements dans la structure normalisée"""
    try:
        cursor = connexion.cursor()
        total_rows = len(df)
        batches = (total_rows // batch_size) + (1 if total_rows % batch_size > 0 else 0)
        
        logging.info(f"Début de l'insertion des données ({total_rows} lignes, {batches} lots)")
        start_time = time.time()
        successful_inserts = 0
        skipped_duplicates = 0
        coordinate_errors = 0
        
        for i in range(batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_rows)
            batch = df.iloc[start_idx:end_idx]
            
            try:
                connexion.start_transaction()
                
                for idx, row in batch.iterrows():
                    try:
                        # Vérifier l'unicité du code établissement
                        code_etab = str(row.get('code_etablissement', ''))
                        if not code_etab:
                            logging.warning(f"Ligne {idx}: Code établissement manquant, ignorée")
                            continue
                        
                        existing_id = check_etablissement_exists(cursor, code_etab)
                        if existing_id:
                            skipped_duplicates += 1
                            logging.debug(f"Établissement {code_etab} déjà existant, ignoré")
                            continue
                        
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
                        
                        # 2. Insérer la localisation (sans coordonnées)
                        localisation_id = insert_localisation(cursor, row)
                        
                        # 3. Valider et préparer les coordonnées pour l'établissement
                        latitude, longitude = validate_coordinates(
                            row.get('latitude'), 
                            row.get('longitude')
                        )
                        
                        if latitude is None and longitude is None:
                            coordinate_errors += 1
                            logging.debug(f"Ligne {idx}: Coordonnées invalides pour {code_etab}")
                        
                        # 4. Vérifier que les IDs obligatoires existent
                        if not all([localisation_id, milieu_id, statut_id, systeme_id]):
                            logging.warning(f"Ligne {idx}: IDs de référence manquants, ignorée")
                            continue
                        
                        # 5. Insérer l'établissement AVEC coordonnées validées
                        cursor.execute("""
                            INSERT INTO etablissements (code_etablissement, nom_etablissement,
                                                       localisation_id, milieu_id, statut_id, 
                                                       systeme_id, annee_id, latitude, longitude)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            code_etab,
                            str(row.get('nom_etablissement', '')),
                            localisation_id,
                            milieu_id,
                            statut_id,
                            systeme_id,
                            annee_id,
                            latitude,
                            longitude
                        ))
                        
                        etablissement_id = cursor.lastrowid
                        
                        # 6. Insérer les équipements avec conversion booléenne sûre
                        cursor.execute("""
                            INSERT INTO equipements_etablissement (etablissement_id, existe_elect,
                                                                  existe_latrine, existe_latrine_fonct,
                                                                  acces_toute_saison, eau)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            etablissement_id,
                            convert_to_boolean(row.get('existe_elect', False)),
                            convert_to_boolean(row.get('existe_latrine', False)),
                            convert_to_boolean(row.get('existe_latrine_fonct', False)),
                            convert_to_boolean(row.get('acces_toute_saison', False)),
                            convert_to_boolean(row.get('eau', False))
                        ))
                        
                        # 7. Insérer les effectifs avec noms de colonnes corrigés
                        cursor.execute("""
                            INSERT INTO effectifs (etablissement_id, sommedenb_eff_g, sommedenb_eff_f,
                                                 tot, sommedenb_ens_h, sommedenb_ens_f, total_ense)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            etablissement_id,
                            int(float(row.get('sommedenb_eff_g', 0))),
                            int(float(row.get('sommedenb_eff_f', 0))),
                            int(float(row.get('tot', 0))),
                            int(float(row.get('sommedenb_ens_h', 0))),
                            int(float(row.get('sommedenb_ens_f', 0))),
                            int(float(row.get('total_ense', 0)))
                        ))
                        
                        # 8. Insérer les infrastructures
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
                        logging.error(f"Erreur avec la ligne {idx} (code: {code_etab}): {e}")
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
        logging.info(f"Doublons ignorés: {skipped_duplicates}")
        logging.info(f"Erreurs de coordonnées: {coordinate_errors}")
        
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
    fichier_excel = 'C:/Users/_Salim_mevtr_/project-lab/edumap-api_v0.1/etl/Base_2024.xlsx'
    
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
        
        # Afficher les colonnes originales
        logging.info(f"Colonnes originales: {list(df.columns)}")
        
        # Mapper les colonnes
        df = map_columns(df)
        
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