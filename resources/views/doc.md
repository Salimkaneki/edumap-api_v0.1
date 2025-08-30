# Documentation des Contrôleurs d'Établissements

## Vue d'ensemble

Le système utilise maintenant **deux contrôleurs séparés** pour gérer les établissements :

1. **`PublicEtablissementController`** - Pour l'accès public (consultation uniquement)
2. **`AdminEtablissementController`** - Pour l'accès administrateur (CRUD complet)

## PublicEtablissementController

### Fonctionnalités
- **Accès public** (pas d'authentification requise)
- **Consultation uniquement** (lecture seule)
- **Cache optimisé** pour les performances
- **Données filtrées** (seulement les informations publiques)

### Endpoints disponibles

```php
GET /api/etablissements                    // Liste paginée
GET /api/etablissements/search             // Recherche avec filtres
GET /api/etablissements/map                // Données pour carte
GET /api/etablissements/stats              // Statistiques publiques
GET /api/etablissements/filter-options     // Options de filtrage
GET /api/etablissements/{id}               // Détails d'un établissement
```

### Caractéristiques techniques
- Cache de **5 minutes** pour les listes
- Cache de **10 minutes** pour les cartes
- Cache de **1 heure** pour les statistiques
- Limitation à **50 éléments par page** maximum
- Sélection des champs optimisée
- Gestion d'erreurs simplifiée

## AdminEtablissementController

### Fonctionnalités
- **Accès protégé** (authentification admin requise)
- **CRUD complet** (Create, Read, Update, Delete)
- **Logs détaillés** de toutes les actions
- **Validation renforcée** des données
- **Historique des modifications**

### Endpoints disponibles

```php
// Consultation (admin)
GET /api/admin/etablissements                    // Liste paginée admin
GET /api/admin/etablissements/search             // Recherche admin
GET /api/admin/etablissements/map                // Carte admin
GET /api/admin/etablissements/stats              // Statistiques détaillées
GET /api/admin/etablissements/filter-options     // Options de filtrage
GET /api/admin/etablissements/export             // Export des données
GET /api/admin/etablissements/{id}               // Détails complets
GET /api/admin/etablissements/{id}/history       // Historique des modifications

// Modification (admin et superadmin)
POST /api/admin/etablissements                   // Créer un établissement
PUT /api/admin/etablissements/{id}               // Modifier un établissement
PATCH /api/admin/etablissements/{id}             // Modification partielle

// Suppression (superadmin uniquement)
DELETE /api/admin/etablissements/{id}            // Supprimer un établissement
```

### Caractéristiques techniques
- Cache de **5 minutes** pour les listes
- Cache de **30 minutes** pour les statistiques
- Limitation à **100 éléments par page** maximum
- Logs détaillés de toutes les actions
- Validation stricte des données d'entrée
- Gestion des relations avec les modèles liés

## Différences principales

| Aspect | PublicEtablissementController | AdminEtablissementController |
|--------|------------------------------|------------------------------|
| **Authentification** | Aucune | Requise (admin) |
| **Permissions** | Lecture seule | CRUD complet |
| **Cache** | Long (optimisé) | Court (données fraîches) |
| **Données** | Filtrées | Complètes |
| **Logs** | Basiques | Détaillés |
| **Validation** | Aucune | Stricte |
| **Pagination** | Max 50 | Max 100 |

## Middlewares utilisés

### Pour les routes publiques
```php
// Aucun middleware requis
Route::get('/etablissements', [PublicEtablissementController::class, 'index']);
```

### Pour les routes admin
```php
// Middleware admin requis
Route::middleware(['auth:sanctum', 'admin'])->group(function () {
    Route::get('/admin/etablissements', [AdminEtablissementController::class, 'index']);
});

// Middleware superadmin pour la suppression
Route::middleware(['auth:sanctum', 'admin', 'superadmin'])->group(function () {
    Route::delete('/admin/etablissements/{id}', [AdminEtablissementController::class, 'destroy']);
});
```

## Exemples d'utilisation

### Frontend public
```javascript
// Récupérer la liste des établissements
fetch('/api/etablissements?page=1&per_page=20')
  .then(response => response.json())
  .then(data => console.log(data));

// Rechercher des établissements
fetch('/api/etablissements/search?region=CENTRALE&libelle_type_milieu=Rural')
  .then(response => response.json())
  .then(data => console.log(data));
```

### Frontend admin
```javascript
// Récupérer la liste admin (avec token)
fetch('/api/admin/etablissements', {
  headers: {
    'Authorization': 'Bearer ' + token,
    'Content-Type': 'application/json'
  }
})
.then(response => response.json())
.then(data => console.log(data));

// Créer un établissement
fetch('/api/admin/etablissements', {
  method: 'POST',
  headers: {
    'Authorization': 'Bearer ' + token,
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    code_etablissement: 'ETB001',
    nom_etablissement: 'École Primaire Test',
    // ... autres données
  })
});
```

## Avantages de cette architecture

1. **Séparation des responsabilités** : Code public vs admin séparé
2. **Sécurité renforcée** : Pas d'exposition accidentelle de données sensibles
3. **Performance optimisée** : Cache adapté à chaque usage
4. **Maintenance facilitée** : Modifications indépendantes
5. **Évolutivité** : Facile d'ajouter de nouvelles fonctionnalités

## Migration depuis l'ancien contrôleur

Pour migrer depuis l'ancien `EtablissementController` :

1. **Routes publiques** → Utiliser `PublicEtablissementController`
2. **Routes admin** → Utiliser `AdminEtablissementController`
3. **Middleware** → Vérifier que les bons middlewares sont appliqués
4. **Frontend** → Mettre à jour les URLs d'API

## Recommandations

1. **Utilisez le contrôleur public** pour toutes les consultations non-admin
2. **Réservez le contrôleur admin** pour les opérations d'administration
3. **Test