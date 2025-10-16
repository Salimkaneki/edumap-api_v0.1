# Documentation Export des Établissements - EduMap API

## Vue d'ensemble

L'API EduMap permet d'exporter les données des établissements scolaires en trois formats différents :

-   **Excel (.xlsx)** - Format idéal pour l'analyse de données
-   **CSV (.csv)** - Format compatible avec tous les tableurs
-   **PDF (.pdf)** - Format idéal pour l'impression et l'archivage

## Endpoint d'export

```
GET /api/admin/etablissements/export
```

### Authentification requise

-   **Type** : Bearer Token (Sanctum)
-   **Rôle** : Admin ou SuperAdmin

## Paramètres de requête

### Obligatoire

| Paramètre | Type   | Description              | Valeurs acceptées     |
| --------- | ------ | ------------------------ | --------------------- |
| `format`  | string | Format d'export souhaité | `excel`, `csv`, `pdf` |

### Optionnels (Filtres)

| Paramètre                  | Type   | Description                | Exemple                                                    |
| -------------------------- | ------ | -------------------------- | ---------------------------------------------------------- |
| `region`                   | string | Filtrer par région         | `MARITIME`, `KARA`, `CENTRALE`, `PLATEAUX`, `SAVANES`      |
| `prefecture`               | string | Filtrer par préfecture     | `Golfe`, `Kozah`, etc.                                     |
| `libelle_type_milieu`      | string | Filtrer par type de milieu | `Rural`, `Semi Urbain`, `Urbain`                           |
| `libelle_type_statut_etab` | string | Filtrer par statut         | `Public`, `Privé Catholique`, etc.                         |
| `libelle_type_systeme`     | string | Filtrer par système        | `PRIMAIRE`, `SECONDAIRE I`, `SECONDAIRE II`, `PRESCOLAIRE` |

## Exemples d'utilisation

### 1. Export Excel de tous les établissements

#### cURL

```bash
curl -X GET "http://localhost:8000/api/admin/etablissements/export?format=excel" \
  -H "Authorization: Bearer votre_token_ici" \
  -o etablissements.xlsx
```

#### JavaScript (Fetch)

```javascript
fetch("http://localhost:8000/api/admin/etablissements/export?format=excel", {
    headers: {
        Authorization: "Bearer votre_token_ici",
    },
})
    .then((response) => response.blob())
    .then((blob) => {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "etablissements.xlsx";
        a.click();
    });
```

#### Postman

1. Méthode : **GET**
2. URL : `http://localhost:8000/api/admin/etablissements/export?format=excel`
3. Headers : `Authorization: Bearer {token}`
4. Cliquez sur **Send and Download**

### 2. Export CSV filtré par région

```bash
curl -X GET "http://localhost:8000/api/admin/etablissements/export?format=csv&region=MARITIME" \
  -H "Authorization: Bearer votre_token_ici" \
  -o etablissements_maritime.csv
```

### 3. Export PDF avec plusieurs filtres

```bash
curl -X GET "http://localhost:8000/api/admin/etablissements/export?format=pdf&region=KARA&libelle_type_systeme=PRIMAIRE&libelle_type_milieu=Urbain" \
  -H "Authorization: Bearer votre_token_ici" \
  -o etablissements_kara_primaire.pdf
```

### 4. Export Excel des établissements publics

```bash
curl -X GET "http://localhost:8000/api/admin/etablissements/export?format=excel&libelle_type_statut_etab=Public" \
  -H "Authorization: Bearer votre_token_ici" \
  -o etablissements_publics.xlsx
```

## Colonnes exportées

### Excel et CSV (27 colonnes)

1. **Code Établissement** - Code unique
2. **Nom Établissement** - Nom complet
3. **Région** - Région administrative
4. **Préfecture** - Préfecture
5. **Canton/Village Autonome** - Canton ou village autonome
6. **Ville/Village/Quartier** - Localisation précise
7. **Commune** - Commune
8. **Latitude** - Coordonnée GPS
9. **Longitude** - Coordonnée GPS
10. **Type de Milieu** - Rural, Semi Urbain, Urbain
11. **Statut** - Type d'établissement
12. **Système** - Niveau d'enseignement
13. **Année** - Année scolaire
14. **Électricité** - Oui/Non
15. **Latrines** - Oui/Non
16. **Latrines Fonctionnelles** - Oui/Non
17. **Accès Toute Saison** - Oui/Non
18. **Eau** - Oui/Non
19. **Effectif Garçons** - Nombre
20. **Effectif Filles** - Nombre
21. **Total Élèves** - Nombre
22. **Enseignants Hommes** - Nombre
23. **Enseignants Femmes** - Nombre
24. **Total Enseignants** - Nombre
25. **Salles en Dur** - Nombre
26. **Salles en Banco** - Nombre
27. **Autres Salles** - Nombre

### PDF (Vue synthétique)

Le PDF contient :

-   **En-tête** : Titre, date de génération
-   **Filtres appliqués** : Résumé des filtres utilisés
-   **Statistiques globales** :
    -   Total établissements
    -   Total élèves
    -   Total enseignants
    -   Nombre avec électricité
    -   Nombre avec eau
-   **Tableau des établissements** :
    -   Code, Nom, Région, Préfecture
    -   Milieu, Statut, Système
    -   Effectifs, Enseignants
    -   Indicateurs d'équipements et infrastructures

## Format de réponse

### Succès

Le serveur retourne directement le fichier avec les en-têtes appropriés :

**Excel (.xlsx)**

```
Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
Content-Disposition: attachment; filename=etablissements_2025-10-15_143052.xlsx
```

**CSV (.csv)**

```
Content-Type: text/csv
Content-Disposition: attachment; filename=etablissements_2025-10-15_143052.csv
```

**PDF (.pdf)**

```
Content-Type: application/pdf
Content-Disposition: attachment; filename=etablissements_2025-10-15_143052.pdf
```

### Erreurs

#### Paramètres invalides (422)

```json
{
    "error": "Paramètres invalides",
    "details": {
        "format": ["Le format doit être excel, csv ou pdf"]
    }
}
```

#### Non authentifié (401)

```json
{
    "message": "Unauthenticated."
}
```

#### Accès interdit (403)

```json
{
    "error": "Accès non autorisé"
}
```

#### Erreur serveur (500)

```json
{
    "error": "Une erreur est survenue lors de l'export",
    "message": "Description de l'erreur"
}
```

## Caractéristiques des formats

### Excel (.xlsx)

✅ **Avantages** :

-   Format professionnel
-   Colonnes avec largeurs optimisées
-   En-tête avec mise en forme (couleur bleue, texte blanc)
-   Compatible avec Excel, LibreOffice, Google Sheets
-   Support des formules et graphiques

📊 **Idéal pour** :

-   Analyse de données
-   Création de graphiques
-   Partage avec des équipes

### CSV (.csv)

✅ **Avantages** :

-   Format universel
-   Léger et rapide
-   Compatible avec tous les systèmes
-   Facile à importer dans d'autres applications

📊 **Idéal pour** :

-   Import dans d'autres systèmes
-   Traitement automatisé
-   Intégration avec des scripts

### PDF (.pdf)

✅ **Avantages** :

-   Format de présentation
-   Mise en page professionnelle
-   Statistiques visuelles
-   Non modifiable (intégrité des données)
-   Adapté à l'impression

📊 **Idéal pour** :

-   Rapports officiels
-   Archivage
-   Impression
-   Présentation aux autorités

## Bonnes pratiques

### 1. Filtrage

-   Utilisez les filtres pour réduire la taille des exports
-   Les exports non filtrés peuvent être volumineux

### 2. Format approprié

-   **Excel** : Pour l'analyse et manipulation des données
-   **CSV** : Pour l'intégration avec d'autres systèmes
-   **PDF** : Pour les rapports et l'archivage

### 3. Nom de fichier

Les fichiers sont automatiquement nommés avec la date et l'heure :

```
etablissements_2025-10-15_143052.xlsx
etablissements_2025-10-15_143052.csv
etablissements_2025-10-15_143052.pdf
```

### 4. Performance

-   Les exports de grandes quantités de données peuvent prendre du temps
-   Préférez exporter pendant les heures creuses pour les gros volumes

## Exemples d'intégration

### React/Next.js

```javascript
const exportData = async (format, filters = {}) => {
    const params = new URLSearchParams({ format, ...filters });

    try {
        const response = await fetch(
            `${API_URL}/api/admin/etablissements/export?${params}`,
            {
                headers: {
                    Authorization: `Bearer ${token}`,
                },
            }
        );

        if (!response.ok) throw new Error("Export failed");

        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `etablissements.${format === "excel" ? "xlsx" : format}`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
    } catch (error) {
        console.error("Error exporting:", error);
    }
};

// Utilisation
exportData("excel", { region: "MARITIME" });
```

### Vue.js

```javascript
async exportToExcel() {
  const response = await axios({
    url: '/api/admin/etablissements/export',
    method: 'GET',
    params: {
      format: 'excel',
      region: this.selectedRegion
    },
    responseType: 'blob',
    headers: {
      'Authorization': `Bearer ${this.token}`
    }
  });

  const url = window.URL.createObjectURL(new Blob([response.data]));
  const link = document.createElement('a');
  link.href = url;
  link.setAttribute('download', 'etablissements.xlsx');
  document.body.appendChild(link);
  link.click();
}
```

## Support

Pour toute question ou problème concernant l'export :

1. Vérifiez que votre token est valide
2. Assurez-vous d'avoir les permissions nécessaires
3. Consultez les logs du serveur en cas d'erreur
4. Contactez l'équipe de développement

## Notes techniques

-   **Limite de mémoire** : Les exports de très gros volumes (>10 000 établissements) peuvent nécessiter une augmentation de la limite de mémoire PHP
-   **Timeout** : Pour les gros exports, le timeout peut être augmenté dans la configuration PHP
-   **Cache** : Les exports ne sont pas mis en cache pour garantir des données à jour
