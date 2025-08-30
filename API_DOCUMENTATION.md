# 📚 **DOCUMENTATION API EDUMAP v1.0**

## 🎯 **VUE D'ENSEMBLE**

L'API EduMap permet de gérer et consulter les données de **15 216 établissements scolaires du Togo** avec :
- **API publique** (sans authentification) pour consultation
- **API admin** (avec authentification) pour gestion complète
- **Géolocalisation** et recherche par proximité
- **Filtres avancés** par région, statut, infrastructures
- **Statistiques** détaillées

---

## 🔗 **ENDPOINTS PUBLICS** (sans authentification)

### **1. Liste des établissements**
```http
GET /api/etablissements
```
**Paramètres :**
- `page` (int) : Numéro de page (défaut: 1)
- `per_page` (int) : Éléments par page (max: 100, défaut: 20)

**Réponse :**
```json
{
  "data": [...],
  "links": {...},
  "meta": {...}
}
```

### **2. Détails d'un établissement**
```http
GET /api/etablissements/{id}
```

### **3. Recherche d'établissements**
```http
GET /api/etablissements/search?nom_etablissement=École&region=Maritime
```
**Paramètres de filtre :**
- `nom_etablissement` : Recherche dans le nom
- `region` : Filtrer par région
- `prefecture` : Filtrer par préfecture
- `statut` : Public, Privé laïc, Privé confessionnel
- `milieu` : Rural, Urbain
- `systeme` : PRESCOLAIRE, PRIMAIRE, SECONDAIRE I, SECONDAIRE II
- `existe_elect` : true/false (électricité)
- `existe_latrine` : true/false (latrines)
- `eau` : true/false (eau potable)

### **4. Données pour carte interactive**
```http
GET /api/etablissements/map
```
Retourne tous les établissements avec coordonnées GPS.

### **5. Établissements à proximité**
```http
POST /api/etablissements/nearby
```
**Corps de la requête :**
```json
{
  "lat": 6.1319,
  "lng": 1.2228,
  "radius": 10
}
```

### **6. Statistiques publiques**
```http
GET /api/etablissements/stats
```

### **7. Options de filtrage**
```http
GET /api/etablissements/filter-options
```

---

## 🔐 **ENDPOINTS ADMIN** (avec authentification)

### **Authentification**

#### **Connexion admin**
```http
POST /api/admin/login
```
**Corps :**
```json
{
  "email": "admin@edumap.tg",
  "password": "password"
}
```

#### **Profil admin**
```http
GET /api/admin/me
Authorization: Bearer {token}
```

#### **Déconnexion**
```http
POST /api/admin/logout
Authorization: Bearer {token}
```

### **Gestion des établissements (Admin)**

#### **Créer un établissement**
```http
POST /api/admin/etablissements
Authorization: Bearer {token}
```
**Corps :**
```json
{
  "code_etablissement": "ETB001",
  "nom_etablissement": "École Primaire de Test",
  "latitude": 6.1319,
  "longitude": 1.2228,
  "region": "Maritime",
  "prefecture": "Golfe",
  "canton_village_autonome": "Test Village",
  "ville_village_quartier": "Test Quartier",
  "libelle_type_milieu": "Urbain",
  "libelle_type_statut_etab": "Public",
  "libelle_type_systeme": "PRIMAIRE",
  "existe_elect": true,
  "existe_latrine": true,
  "eau": true,
  "sommedenb_eff_g": 120,
  "sommedenb_eff_f": 110,
  "sommedenb_ens_h": 5,
  "sommedenb_ens_f": 8
}
```

#### **Modifier un établissement**
```http
PUT /api/admin/etablissements/{id}
Authorization: Bearer {token}
```

#### **Import Excel**
```http
POST /api/admin/etablissements/import
Authorization: Bearer {token}
Content-Type: multipart/form-data
```
**Corps :**
```
file: [fichier Excel]
```

#### **Export des données**
```http
GET /api/admin/etablissements/export
Authorization: Bearer {token}
```

### **SuperAdmin uniquement**

#### **Supprimer un établissement**
```http
DELETE /api/admin/etablissements/{id}
Authorization: Bearer {token}
```

#### **Gestion des admins**
```http
GET /api/admin/admins
POST /api/admin/admins
Authorization: Bearer {token}
```

---

## 🗂️ **STRUCTURE DES DONNÉES**

### **Établissement complet**
```json
{
  "id": 1,
  "code_etablissement": "ETB001",
  "nom_etablissement": "École Primaire Exemple",
  "latitude": 6.1319,
  "longitude": 1.2228,
  "localisation": {
    "region": "Maritime",
    "prefecture": "Golfe",
    "canton_village_autonome": "Village",
    "ville_village_quartier": "Quartier",
    "commune_etab": "Commune"
  },
  "milieu": {
    "libelle_type_milieu": "Urbain"
  },
  "statut": {
    "libelle_type_statut_etab": "Public"
  },
  "systeme": {
    "libelle_type_systeme": "PRIMAIRE"
  },
  "effectif": {
    "sommedenb_eff_g": 120,
    "sommedenb_eff_f": 110,
    "tot": 230,
    "sommedenb_ens_h": 5,
    "sommedenb_ens_f": 8,
    "total_ense": 13
  },
  "infrastructure": {
    "sommedenb_salles_classes_dur": 6,
    "sommedenb_salles_classes_banco": 2,
    "sommedenb_salles_classes_autre": 0,
    "total_salles": 8
  },
  "equipement": {
    "existe_elect": true,
    "existe_latrine": true,
    "existe_latrine_fonct": true,
    "acces_toute_saison": true,
    "eau": true
  }
}
```

---

## 🎛️ **GUIDE D'UTILISATION**

### **Pour une application cliente (consultation)**

1. **Lister les établissements** : `GET /api/etablissements`
2. **Afficher la carte** : `GET /api/etablissements/map`
3. **Recherche par nom** : `GET /api/etablissements/search?nom_etablissement=...`
4. **Filtrer par région** : `GET /api/etablissements/search?region=Maritime`
5. **Établissements proches** : `POST /api/etablissements/nearby`

### **Pour une interface admin**

1. **Se connecter** : `POST /api/admin/login`
2. **Ajouter établissement** : `POST /api/admin/etablissements`
3. **Importer Excel** : `POST /api/admin/etablissements/import`
4. **Gérer les données** : CRUD complet via `/api/admin/etablissements/*`

---

## 🔧 **INSTALLATION & CONFIGURATION**

### **1. Migrations & Seeders**
```bash
php artisan migrate
php artisan db:seed --class=ReferenceDataSeeder
php artisan db:seed --class=AdminSeeder
```

### **2. Import des données Excel**
```bash
# Via l'interface admin ou directement :
curl -X POST http://localhost:8000/api/admin/etablissements/import \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@Base_2024.xlsx"
```

### **3. Tests des endpoints**
```bash
# Test public
curl http://localhost:8000/api/etablissements

# Test admin
curl -H "Authorization: Bearer TOKEN" \
  http://localhost:8000/api/admin/etablissements/stats
```

---

## 📊 **PERFORMANCES & OPTIMISATIONS**

- **Cache** : 5 minutes pour les données publiques, 1h pour les statistiques
- **Pagination** : Maximum 100 éléments par page
- **Index DB** : Sur latitude/longitude pour recherches géographiques
- **Lazy loading** : Relations chargées automatiquement
- **Compression** : Données compressées en JSON

---

## 🚀 **PROCHAINES FONCTIONNALITÉS**

- [ ] API de géocodage inversé
- [ ] Notifications en temps réel
- [ ] Export PDF des rapports
- [ ] API de comparaison d'établissements
- [ ] Système de favoris utilisateur
- [ ] Historique des modifications

---

## 📞 **SUPPORT**

- **Documentation** : Cette API suit les standards REST
- **Formats** : JSON uniquement
- **Encodage** : UTF-8
- **CORS** : Configuré pour développement local

**Contact** : support@edumap.tg
