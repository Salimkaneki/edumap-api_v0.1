# Template d'Import des Établissements - Guide d'utilisation

## Fichier créé : `template_import_etablissements.csv`

Ce fichier CSV contient 10 établissements d'exemple pour vous aider à comprendre le format d'import.

## Comment utiliser ce fichier

### Option 1 : Ouvrir avec Excel

1. Double-cliquez sur `template_import_etablissements.csv`
2. Le fichier s'ouvrira dans Excel
3. Modifiez les données selon vos besoins
4. Enregistrez-le sous format `.xlsx` ou `.csv`

### Option 2 : Importer dans Excel

1. Ouvrez Excel
2. Allez dans `Données` → `À partir d'un fichier texte/CSV`
3. Sélectionnez `template_import_etablissements.csv`
4. Suivez l'assistant d'importation
5. Modifiez et enregistrez

## Structure du fichier

Le fichier contient **27 colonnes** avec **10 enregistrements d'exemple** :

### Établissements inclus :

1. **LOME001** - EPP Centre Lomé (MARITIME, Golfe)
2. **KARA001** - Collège Moderne de Kara (KARA, Kozah)
3. **SAV001** - EPP Sarakawa (KARA, Kozah)
4. **SOKO001** - École Catholique Sokodé (CENTRALE, Tchaoudjo)
5. **ATAL001** - EPP Atakpamé Centre (PLATEAUX, Ogou)
6. **BASA001** - Collège Protestant de Bassar (KARA, Bassar)
7. **DAPA001** - EPP Dapaong (SAVANES, Tône)
8. **MANG001** - Lycée Moderne de Mango (SAVANES, Oti)
9. **KPAL001** - EPP Kpalimé (PLATEAUX, Kloto)
10. **TABA001** - École Communautaire Tabligbo (MARITIME, Yoto)

## Colonnes du fichier

### Colonnes obligatoires (2)

-   `Code_etab` - Code unique
-   `Nom_etab` - Nom de l'établissement

### Colonnes de localisation (7)

-   `Region`
-   `Prefecture`
-   `Canton_ou_village_autonome`
-   `Ville_village_quartier`
-   `Commune_etab`
-   `Latitude`
-   `Longitude`

### Colonnes de classification (4)

-   `Libelle_type_milieu` (Rural, Semi Urbain, Urbain)
-   `Libelle_type_statut_etab` (Public, Privé Catholique, Privé Protestant, Communautaire, etc.)
-   `Libelle_type_systeme` (PRIMAIRE, SECONDAIRE I, SECONDAIRE II, PRESCOLAIRE)
-   `Libelle_type_annee` (ex: 2023-2024)

### Colonnes d'équipements (5)

-   `Existe_elect` (oui/non)
-   `Existe_latrine` (oui/non)
-   `Existe_latrine_fonct` (oui/non)
-   `Acces_toute_saison` (oui/non)
-   `Eau` (oui/non)

### Colonnes d'effectifs (6)

-   `SommedenbEffG` (élèves garçons)
-   `SommedenbEffF` (élèves filles)
-   `Tot` (total élèves)
-   `SommedenbEnsH` (enseignants hommes)
-   `SommedenbEnsF` (enseignants femmes)
-   `TotalEnse` (total enseignants)

### Colonnes d'infrastructures (3)

-   `SommedenbSallesClassesDur` (salles en dur)
-   `SommedenbSallesClassesBanco` (salles en banco)
-   `SommedenbSallesClassesAutre` (autres types)

## Valeurs acceptées

### Libelle_type_milieu

-   Rural
-   Semi Urbain
-   Urbain

### Libelle_type_statut_etab

-   Public
-   Privé Catholique
-   Privé Islamique
-   Privé Laïc
-   Privé Protestant
-   Communautaire

### Libelle_type_systeme

-   PRIMAIRE
-   PRESCOLAIRE
-   SECONDAIRE I
-   SECONDAIRE II

### Champs booléens

-   oui / non
-   yes / no
-   1 / 0
-   true / false
-   vrai / faux

## Import dans l'API

### Endpoint

```
POST /api/admin/etablissements/import
```

### Headers

```
Authorization: Bearer {votre_token_admin}
Content-Type: multipart/form-data
```

### Body

```
file: template_import_etablissements.csv (ou .xlsx)
```

### Exemple avec cURL

```bash
curl -X POST "http://localhost:8000/api/admin/etablissements/import" \
  -H "Authorization: Bearer votre_token_ici" \
  -F "file=@template_import_etablissements.csv"
```

### Exemple avec Postman

1. Méthode : POST
2. URL : `http://localhost:8000/api/admin/etablissements/import`
3. Headers : `Authorization: Bearer {token}`
4. Body :
    - Type : form-data
    - Key : `file`
    - Type : File
    - Value : Sélectionnez votre fichier

## Réponse attendue

### Succès

```json
{
    "message": "Import terminé avec succès",
    "statistics": {
        "imported": 10,
        "skipped": 0,
        "errors": 0
    },
    "errors": []
}
```

### Avec erreurs

```json
{
    "message": "Import terminé avec succès",
    "statistics": {
        "imported": 8,
        "skipped": 1,
        "errors": 1
    },
    "errors": [
        "Ligne 3: Code ou nom d'établissement manquant",
        "Ligne 5: Type de milieu invalide: Urbaine"
    ]
}
```

## Conseils d'utilisation

1. **Testez d'abord** avec quelques lignes avant d'importer un gros fichier
2. **Vérifiez les doublons** : les codes déjà existants seront ignorés
3. **Respectez les valeurs exactes** pour les champs de classification
4. **Coordonnées GPS** : utilisez le format décimal (pas de degrés/minutes/secondes)
5. **Fichiers volumineux** : Limitez à 10 MB maximum
6. **Encodage** : Utilisez UTF-8 pour éviter les problèmes d'accents

## Modification du template

Pour ajouter vos propres données :

1. Ouvrez le fichier dans Excel
2. Supprimez les lignes d'exemple (gardez la ligne d'en-tête !)
3. Ajoutez vos données en respectant le format
4. Enregistrez et importez via l'API

## Support

Pour toute question ou problème d'import, consultez les logs de l'API ou contactez l'équipe de développement.
