# Payload pour Créer un Établissement - EduMap API

## Payload JSON complet pour créer u## Notes importantes

1. **Authentification requise** : L'utilisateur doit être authentifié en tant qu'admin
2. **Calculs automatiques** : Si `tot` ou `total_ense` ne sont pas fournis, ils sont calculés automatiquement :
   - `tot` = `sommedenb_eff_g` + `sommedenb_eff_f`
   - `total_ense` = `sommedenb_ens_h` + `sommedenb_ens_f`
3. **Création automatique des enregistrements liés** : Si vous fournissez des données d'effectifs, d'équipements ou d'infrastructures, les enregistrements correspondants seront automatiquement créés dans les tables `effectifs`, `equipements_etablissement` et `infrastructures`
4. **Mapping automatique** : Les libellés (`libelle_type_milieu`, etc.) sont automatiquement convertis en IDs de base de données
5. **Unicité** : `code_etablissement` doit être unique dans toute la base de données
6. **Types de données stricts** : Les valeurs boolean doivent être `true`/`false`, les nombres doivent être numériques
7. **Valeurs par défaut** : Les champs numériques non définis retournent `0` au lieu de `null` dans les réponses JSONement :

```json
{
    "code_etablissement": "ETAB001",
    "nom_etablissement": "École Primaire Publique de Test",
    "region": "Région Maritime",
    "prefecture": "Préfecture de Lomé",
    "canton_village_autonome": "Canton Central",
    "ville_village_quartier": "Quartier Administratif",
    "commune_etab": "Commune de Lomé",
    "libelle_type_milieu": "Urbain",
    "libelle_type_statut_etab": "Public",
    "libelle_type_systeme": "PRIMAIRE",
    "libelle_type_annee": "2024-2025",
    "existe_elect": true,
    "existe_latrine": true,
    "existe_latrine_fonct": true,
    "acces_toute_saison": true,
    "eau": true,
    "latitude": 6.1725,
    "longitude": 1.2314,
    "sommedenb_eff_g": 150,
    "sommedenb_eff_f": 145,
    "tot": 295,
    "sommedenb_ens_h": 8,
    "sommedenb_ens_f": 7,
    "total_ense": 15,
    "sommedenb_salles_classes_dur": 12,
    "sommedenb_salles_classes_banco": 3,
    "sommedenb_salles_classes_autre": 1
}
```

## Détail des champs et valeurs possibles

### Champs obligatoires

| Champ                      | Type               | Valeurs possibles                                                                                              | Description                             |
| -------------------------- | ------------------ | -------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| `code_etablissement`       | `string`           | Toute chaîne unique                                                                                            | Code unique identifiant l'établissement |
| `nom_etablissement`        | `string` (max 255) | Toute chaîne descriptive                                                                                       | Nom complet de l'établissement          |
| `region`                   | `string` (max 100) | Toute chaîne                                                                                                   | Nom de la région administrative         |
| `prefecture`               | `string` (max 100) | Toute chaîne                                                                                                   | Nom de la préfecture                    |
| `canton_village_autonome`  | `string` (max 100) | Toute chaîne                                                                                                   | Nom du canton ou village autonome       |
| `ville_village_quartier`   | `string` (max 100) | Toute chaîne                                                                                                   | Nom de la ville, village ou quartier    |
| `libelle_type_milieu`      | `string`           | `"Rural"`, `"Semi Urbain"`, `"Urbain"`                                                                         | Type d'environnement géographique       |
| `libelle_type_statut_etab` | `string`           | `"Communautaire"`, `"Privé Catholique"`, `"Privé Islamique"`, `"Privé Laïc"`, `"Privé Protestant"`, `"Public"` | Statut juridique de l'établissement     |
| `libelle_type_systeme`     | `string`           | `"SECONDAIRE I"`, `"SECONDAIRE II"`, `"PRIMAIRE"`, `"PRESCOLAIRE"`                                             | Niveau d'enseignement                   |
| `latitude`                 | `number`           | Entre -90 et 90                                                                                                | Coordonnée GPS latitude                 |
| `longitude`                | `number`           | Entre -180 et 180                                                                                              | Coordonnée GPS longitude                |

### Champs optionnels

| Champ                | Type               | Valeurs possibles      | Description                      |
| -------------------- | ------------------ | ---------------------- | -------------------------------- |
| `commune_etab`       | `string` (max 100) | Toute chaîne ou `null` | Nom de la commune (optionnel)    |
| `libelle_type_annee` | `string` (max 50)  | Toute chaîne ou `null` | Année scolaire (ex: "2024-2025") |

### Équipements et infrastructures (boolean)

| Champ                  | Type      | Valeurs possibles | Description             |
| ---------------------- | --------- | ----------------- | ----------------------- |
| `existe_elect`         | `boolean` | `true` ou `false` | Accès à l'électricité   |
| `existe_latrine`       | `boolean` | `true` ou `false` | Présence de latrines    |
| `existe_latrine_fonct` | `boolean` | `true` ou `false` | Latrines fonctionnelles |
| `acces_toute_saison`   | `boolean` | `true` ou `false` | Accès toute saison      |
| `eau`                  | `boolean` | `true` ou `false` | Accès à l'eau potable   |

### Effectifs (entiers positifs)

| Champ             | Type      | Valeurs possibles | Description                                    |
| ----------------- | --------- | ----------------- | ---------------------------------------------- |
| `sommedenb_eff_g` | `integer` | ≥ 0               | Nombre d'élèves garçons                        |
| `sommedenb_eff_f` | `integer` | ≥ 0               | Nombre d'élèves filles                         |
| `tot`             | `integer` | ≥ 0               | Total élèves (calculé auto si non fourni)      |
| `sommedenb_ens_h` | `integer` | ≥ 0               | Nombre d'enseignants hommes                    |
| `sommedenb_ens_f` | `integer` | ≥ 0               | Nombre d'enseignantes femmes                   |
| `total_ense`      | `integer` | ≥ 0               | Total enseignants (calculé auto si non fourni) |

### Infrastructures (entiers positifs)

| Champ                            | Type      | Valeurs possibles | Description                            |
| -------------------------------- | --------- | ----------------- | -------------------------------------- |
| `sommedenb_salles_classes_dur`   | `integer` | ≥ 0               | Nombre de salles en matériaux durables |
| `sommedenb_salles_classes_banco` | `integer` | ≥ 0               | Nombre de salles en banco              |
| `sommedenb_salles_classes_autre` | `integer` | ≥ 0               | Nombre d'autres types de salles        |

## Notes importantes

1. **Authentification requise** : L'utilisateur doit être authentifié en tant qu'admin
2. **Calculs automatiques** : Si `tot` ou `total_ense` ne sont pas fournis, ils sont calculés automatiquement :
    - `tot` = `sommedenb_eff_g` + `sommedenb_eff_f`
    - `total_ense` = `sommedenb_ens_h` + `sommedenb_ens_f`
3. **Création automatique des enregistrements liés** : Si vous fournissez des données d'effectifs, d'équipements ou d'infrastructures, les enregistrements correspondants seront automatiquement créés dans les tables `effectifs`, `equipements_etablissement` et `infrastructures`
4. **Mapping automatique** : Les libellés (`libelle_type_milieu`, etc.) sont automatiquement convertis en IDs de base de données
5. **Unicité** : `code_etablissement` doit être unique dans toute la base de données
6. **Types de données stricts** : Les valeurs boolean doivent être `true`/`false`, les nombres doivent être numériques## Exemple minimal (champs obligatoires uniquement)

```json
{
    "code_etablissement": "ETAB001",
    "nom_etablissement": "École Primaire de Test",
    "region": "Région Test",
    "prefecture": "Préfecture Test",
    "canton_village_autonome": "Canton Test",
    "ville_village_quartier": "Quartier Test",
    "libelle_type_milieu": "Urbain",
    "libelle_type_statut_etab": "Public",
    "libelle_type_systeme": "PRIMAIRE",
    "latitude": 6.1725,
    "longitude": 1.2314
}
```

## Exemple avec payload partiel pour mise à jour

```json
{
    "nom_etablissement": "Nouveau nom de l'école",
    "latitude": 6.18,
    "longitude": 1.25,
    "existe_elect": false,
    "sommedenb_eff_g": 200,
    "sommedenb_eff_f": 180
}
```

## Création automatique des données liées

Lors de la création **OU MISE À JOUR** d'un établissement, le système crée automatiquement les enregistrements dans les tables liées si les données correspondantes sont fournies :

### Effectifs (`effectifs`)

Créés automatiquement si au moins un des champs suivants est fourni :

-   `sommedenb_eff_g`, `sommedenb_eff_f`, `tot`, `sommedenb_ens_h`, `sommedenb_ens_f`, `total_ense`

### Équipements (`equipements_etablissement`)

Créés automatiquement si au moins un des champs suivants est fourni :

-   `existe_elect`, `existe_latrine`, `existe_latrine_fonct`, `acces_toute_saison`, `eau`

### Infrastructures (`infrastructures`)

Créés automatiquement si au moins un des champs suivants est fourni :

-   `sommedenb_salles_classes_dur`, `sommedenb_salles_classes_banco`, `sommedenb_salles_classes_autre`

## Endpoint API

-   **Méthode** : `POST`
-   **URL** : `/api/etablissements`
-   **Authentification** : Bearer Token (Admin requis)
-   **Content-Type** : `application/json`
