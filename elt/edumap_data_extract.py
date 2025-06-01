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
    level=logging.DEBUG,  # Niveau DEBUG pour plus de détails
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('import_excel.log'),
        logging.StreamHandler(sys.stdout)  # Afficher aussi dans la console
    ]
)

# Configuration de la connexion à la base de données MySQL
config = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'edumap-api',
    'raise_on_warnings': True
}

def verify_database_table(connexion):
    """Vérifie que la table existe et récupère sa structure"""
    try:
        cursor = connexion.cursor()
        cursor.execute("SHOW TABLES LIKE 'etablissements'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            logging.error("La table 'etablissements' n'existe pas dans la base de données.")
            return False
        
        cursor.execute("DESCRIBE etablissements")
        columns = cursor.fetchall()
        logging.info(f"Structure de la table: {[col[0] for col in columns]}")
        
        return True
    except Error as e:
        logging.error(f"Erreur lors de la vérification de la table: {e}")
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
    
    # Vérifier les types de données
    logging.debug(f"Types de données:\n{df.dtypes}")
    
    # Vérifier les valeurs manquantes
    missing = df.isnull().sum()
    if missing.sum() > 0:
        logging.debug(f"Valeurs manquantes par colonne:\n{missing[missing > 0]}")

def clean_data(df):
    """Nettoie et prépare les données"""
    logging.info("Début du nettoyage des données...")
    
    # Liste des colonnes numériques
    colonnes_numeriques = [
        'sommedenb_eff_g', 'sommedenb_eff_f', 'Tot', 'sommedenb_ens_h', 'sommedenb_ens_f',
        'Total ense', 'sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco',
        'sommedenb_salles_classes_autre'
    ]
    
    # Vérifier si toutes les colonnes sont présentes
    missing_cols = [col for col in colonnes_numeriques if col not in df.columns]
    if missing_cols:
        logging.warning(f"Colonnes manquantes dans le fichier Excel: {missing_cols}")
    
    # Remplacer les valeurs NaN par des valeurs par défaut
    existing_num_cols = [col for col in colonnes_numeriques if col in df.columns]
    df[existing_num_cols] = df[existing_num_cols].fillna(0)  # Remplacer NaN par 0 pour les colonnes numériques
    df = df.fillna('')  # Remplacer NaN par une chaîne vide pour les colonnes textuelles
    
    # Vérifier et nettoyer les coordonnées géographiques
    if 'LATITUDE' in df.columns:
        try:
            # Afficher quelques valeurs pour débogage
            logging.debug(f"Échantillon de LATITUDE avant nettoyage: {df['LATITUDE'].head().tolist()}")
            
            # Convertir en chaîne, remplacer les virgules par des points
            df['LATITUDE'] = df['LATITUDE'].astype(str).str.replace(',', '.')
            # Remplacer les chaînes vides par '0'
            df['LATITUDE'] = df['LATITUDE'].replace('', '0')
            df['LATITUDE'] = df['LATITUDE'].replace('nan', '0')
            
            # Convertir en numérique
            df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce').fillna(0)
            
            # Valider les valeurs
            df['LATITUDE'] = df['LATITUDE'].apply(lambda x: x if -90 <= x <= 90 else 0)
            
            logging.debug(f"Échantillon de LATITUDE après nettoyage: {df['LATITUDE'].head().tolist()}")
        except Exception as e:
            logging.error(f"Erreur lors du nettoyage des latitudes: {e}")
            logging.error(traceback.format_exc())
    else:
        logging.error("Colonne 'LATITUDE' non trouvée dans le fichier Excel")
    
    if 'LONGITUDE' in df.columns:
        try:
            # Afficher quelques valeurs pour débogage
            logging.debug(f"Échantillon de LONGITUDE avant nettoyage: {df['LONGITUDE'].head().tolist()}")
            
            # Convertir en chaîne, remplacer les virgules par des points
            df['LONGITUDE'] = df['LONGITUDE'].astype(str).str.replace(',', '.')
            # Remplacer les chaînes vides par '0'
            df['LONGITUDE'] = df['LONGITUDE'].replace('', '0')
            df['LONGITUDE'] = df['LONGITUDE'].replace('nan', '0')
            
            # Convertir en numérique
            df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce').fillna(0)
            
            # Valider les valeurs
            df['LONGITUDE'] = df['LONGITUDE'].apply(lambda x: x if -180 <= x <= 180 else 0)
            
            logging.debug(f"Échantillon de LONGITUDE après nettoyage: {df['LONGITUDE'].head().tolist()}")
        except Exception as e:
            logging.error(f"Erreur lors du nettoyage des longitudes: {e}")
            logging.error(traceback.format_exc())
    else:
        logging.error("Colonne 'LONGITUDE' non trouvée dans le fichier Excel")
    
    logging.info("Nettoyage des données terminé")
    return df

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

def insert_single_row_test(connexion, df):
    """Teste l'insertion d'une seule ligne pour vérifier la compatibilité"""
    if len(df) == 0:
        logging.error("Le DataFrame est vide, impossible de tester l'insertion")
        return False
    
    try:
        cursor = connexion.cursor()
        row = df.iloc[0]
        
        # Affichage des valeurs pour débogage
        logging.debug("Tentative d'insertion d'une ligne de test:")
        for col in df.columns:
            logging.debug(f"{col}: {row[col]} (type: {type(row[col])})")
        
        # Préparer la requête SQL
        sql = """
        INSERT INTO etablissements (
            latitude, longitude, code_etablissement, libelle_type_milieu, region, prefecture,
            canton_village_autonome, ville_village_quartier, nom_etablissement, libelle_type_statut_etab,
            libelle_type_systeme, existe_elect, existe_latrine, existe_latrine_fonct, acces_toute_saison,
            eau, sommedenb_eff_g, sommedenb_eff_f, tot, sommedenb_ens_h, sommedenb_ens_f, total_ense,
            sommedenb_salles_classes_dur, sommedenb_salles_classes_banco, sommedenb_salles_classes_autre,
            libelle_type_annee, commune_etab
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Exécuter la requête avec les valeurs de la ligne
        valeurs = (
            float(row['LATITUDE']), float(row['LONGITUDE']), str(row['code_etablissement']), str(row['libelle_type_milieu']),
            str(row['region']), str(row['prefecture']), str(row['canton_village_autonome']), str(row['ville_village_quartier']),
            str(row['nom_etablissement']), str(row['libelle_type_statut_etab']), str(row['libelle_type_systeme']),
            str(row['existe_elect']), str(row['existe_latrine']), str(row['existe_latrine_fonct']), str(row['acces_toute_saison']),
            str(row['eau']), float(row['sommedenb_eff_g']), float(row['sommedenb_eff_f']), float(row['Tot']), float(row['sommedenb_ens_h']),
            float(row['sommedenb_ens_f']), float(row['Total ense']), float(row['sommedenb_salles_classes_dur']),
            float(row['sommedenb_salles_classes_banco']), float(row['sommedenb_salles_classes_autre']),
            str(row['libelle_type_annee']), str(row['commune_etab'])
        )
        
        cursor.execute(sql, valeurs)
        connexion.commit()
        
        logging.info("Test d'insertion d'une ligne réussi")
        
        # Supprimer la ligne de test pour éviter les doublons
        cursor.execute(f"DELETE FROM etablissements WHERE code_etablissement = '{str(row['code_etablissement'])}'")
        connexion.commit()
        
        return True
    except Error as e:
        logging.error(f"Erreur lors de l'insertion de test: {e}")
        connexion.rollback()
        return False
    finally:
        cursor.close()

def insert_data_batch(connexion, df, batch_size=100):
    """Insère les données par lots en optimisant avec executemany."""
    try:
        with connexion.cursor() as cursor:
            total_rows = len(df)
            batches = (total_rows // batch_size) + (1 if total_rows % batch_size > 0 else 0)
            
            logging.info(f"Début de l'insertion des données ({total_rows} lignes, {batches} lots)")
            start_time = time.time()
            successful_inserts = 0
            
            # Même SQL que votre script original
            sql = """
            INSERT INTO etablissements (
                latitude, longitude, code_etablissement, libelle_type_milieu, region, prefecture,
                canton_village_autonome, ville_village_quartier, nom_etablissement, libelle_type_statut_etab,
                libelle_type_systeme, existe_elect, existe_latrine, existe_latrine_fonct, acces_toute_saison,
                eau, sommedenb_eff_g, sommedenb_eff_f, tot, sommedenb_ens_h, sommedenb_ens_f, total_ense,
                sommedenb_salles_classes_dur, sommedenb_salles_classes_banco, sommedenb_salles_classes_autre,
                libelle_type_annee, commune_etab
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for i in range(batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_rows)
                batch = df.iloc[start_idx:end_idx]
                
                try:
                    values_list = []
                    for idx, row in batch.iterrows():
                        try:
                            values = (
                                float(row['LATITUDE']), float(row['LONGITUDE']), str(row['code_etablissement']), str(row['libelle_type_milieu']),
                                str(row['region']), str(row['prefecture']), str(row['canton_village_autonome']), str(row['ville_village_quartier']),
                                str(row['nom_etablissement']), str(row['libelle_type_statut_etab']), str(row['libelle_type_systeme']),
                                str(row['existe_elect']), str(row['existe_latrine']), str(row['existe_latrine_fonct']), str(row['acces_toute_saison']),
                                str(row['eau']), float(row['sommedenb_eff_g']), float(row['sommedenb_eff_f']), float(row['Tot']),
                                float(row['sommedenb_ens_h']), float(row['sommedenb_ens_f']), float(row['Total ense']),
                                float(row['sommedenb_salles_classes_dur']), float(row['sommedenb_salles_classes_banco']), float(row['sommedenb_salles_classes_autre']),
                                str(row['libelle_type_annee']), str(row['commune_etab'])
                            )
                            values_list.append(values)
                        except Exception as e:
                            logging.error(f"Erreur avec la ligne {idx}: {e}")
                            # Continuer avec les autres lignes du lot
                
                    # Démarrer une transaction pour ce lot uniquement
                    connexion.start_transaction()
                    if values_list:  # S'assurer qu'il y a des valeurs à insérer
                        cursor.executemany(sql, values_list)
                        connexion.commit()
                        successful_inserts += len(values_list)
                        logging.info(f"Lot {i+1}/{batches} inséré en {time.time() - start_time:.2f} sec (Réussis: {len(values_list)}/{len(batch)})")
                    else:
                        connexion.rollback()
                        logging.warning(f"Lot {i+1}/{batches} n'a pas de données valides à insérer")
                
                except Error as e:
                    connexion.rollback()
                    logging.error(f"Erreur lors de l'insertion du lot {i+1}/{batches}: {e}")

            logging.info(f"Insertion terminée en {time.time() - start_time:.2f} secondes.")
            logging.info(f"Lignes insérées avec succès: {successful_inserts}/{total_rows}")
            
            return successful_inserts > 0
    
    except Error as e:
        connexion.rollback()
        logging.error(f"Erreur lors de l'insertion en lot: {e}")
        return False
    
    """Insère les données par lots pour améliorer les performances"""
    try:
        cursor = connexion.cursor()
        total_rows = len(df)
        batches = (total_rows // batch_size) + (1 if total_rows % batch_size > 0 else 0)
        
        logging.info(f"Début de l'insertion des données ({total_rows} lignes au total, {batches} lots)")
        start_time = time.time()
        
        for i in range(batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_rows)
            batch = df.iloc[start_idx:end_idx]
            
            batch_start_time = time.time()
            # Liste pour stocker toutes les valeurs des lots
            values_list = []
            
            try:
                for _, row in batch.iterrows():
                    # Convertir explicitement chaque valeur au bon type pour éviter les erreurs
                    values = (
                        float(row['LATITUDE']), float(row['LONGITUDE']), str(row['code_etablissement']), str(row['libelle_type_milieu']),
                        str(row['region']), str(row['prefecture']), str(row['canton_village_autonome']), str(row['ville_village_quartier']),
                        str(row['nom_etablissement']), str(row['libelle_type_statut_etab']), str(row['libelle_type_systeme']),
                        str(row['existe_elect']), str(row['existe_latrine']), str(row['existe_latrine_fonct']), str(row['acces_toute_saison']),
                        str(row['eau']), float(row['sommedenb_eff_g']), float(row['sommedenb_eff_f']), float(row['Tot']), float(row['sommedenb_ens_h']),
                        float(row['sommedenb_ens_f']), float(row['Total ense']), float(row['sommedenb_salles_classes_dur']),
                        float(row['sommedenb_salles_classes_banco']), float(row['sommedenb_salles_classes_autre']),
                        str(row['libelle_type_annee']), str(row['commune_etab'])
                    )
                    values_list.append(values)
            except Exception as e:
                logging.error(f"Erreur lors de la préparation des données du lot {i+1}: {e}")
                if len(values_list) == 0:
                    logging.error("Aucune ligne valide à insérer dans ce lot. Passage au lot suivant.")
                    continue
            
            # Préparer la requête SQL
            sql = """
            INSERT INTO etablissements (
                latitude, longitude, code_etablissement, libelle_type_milieu, region, prefecture,
                canton_village_autonome, ville_village_quartier, nom_etablissement, libelle_type_statut_etab,
                libelle_type_systeme, existe_elect, existe_latrine, existe_latrine_fonct, acces_toute_saison,
                eau, sommedenb_eff_g, sommedenb_eff_f, tot, sommedenb_ens_h, sommedenb_ens_f, total_ense,
                sommedenb_salles_classes_dur, sommedenb_salles_classes_banco, sommedenb_salles_classes_autre,
                libelle_type_annee, commune_etab
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            try:
                # Exécuter en mode "executemany" pour optimiser l'insertion par lots
                cursor.executemany(sql, values_list)
                connexion.commit()
                
                batch_time = time.time() - batch_start_time
                percent_complete = (end_idx / total_rows) * 100
                logging.info(f"Lot {i+1}/{batches} : {len(values_list)} lignes insérées en {batch_time:.2f}s ({percent_complete:.1f}% terminé)")
                print(f"Lot {i+1}/{batches} : {len(values_list)} lignes insérées en {batch_time:.2f}s ({percent_complete:.1f}% terminé)")
            except Error as e:
                logging.error(f"Erreur lors de l'insertion du lot {i+1}: {e}")
                connexion.rollback()
                # Continuer avec le prochain lot au lieu d'arrêter tout le processus
                continue
        
        total_time = time.time() - start_time
        logging.info(f"Insertion terminée en {total_time:.2f} secondes.")
        print(f"Insertion terminée en {total_time:.2f} secondes.")
        return True
    except Error as e:
        logging.error(f"Erreur globale lors de l'insertion par lots: {e}")
        connexion.rollback()
        return False
    finally:
        cursor.close()

def main():
    # Chemin du fichier Excel
    # fichier_excel = 'C:/Users/salim_mevtr/Documents/Base_2024.xlsx'
    # fichier_excel = 'C:/Users/Salim_Pereira/Laravel-Project/edumap-api/elt/Base_2024.xlsx'
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
            
            # Vérifier la structure de la table
            if not verify_database_table(connexion):
                logging.error("Problème avec la structure de la table.")
                return
            
            # Tester l'insertion d'une seule ligne
            if not insert_single_row_test(connexion, df):
                logging.error("Le test d'insertion a échoué. Vérifiez la compatibilité des données avec la structure de la table.")
                return
            
            # Insérer les données par lots
            if insert_data_batch(connexion, df):
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