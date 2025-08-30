<?php

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;
use App\Http\Controllers\EtablissementController;
use App\Http\Controllers\AdminAuthController;
use App\Http\Controllers\PublicEtablissementController;

// Route utilisateur standard
Route::get('/user', function (Request $request) {
    return $request->user();
})->middleware('auth:sanctum')->name('user');

// ============================================
// ROUTES PUBLIQUES (Non authentifiées)
// ============================================

// Routes établissements publiques (consultation)
Route::prefix('etablissements')->group(function () {
    // IMPORTANT: Routes spécifiques AVANT les routes avec paramètres
    
    // Recherche d'établissements avec filtres
    Route::get('/search', [PublicEtablissementController::class, 'search']);
    
    // Données pour la carte (coordonnées géographiques)
    Route::get('/map', [PublicEtablissementController::class, 'map']);
    
    // Statistiques globales publiques
    Route::get('/stats', [PublicEtablissementController::class, 'stats']);
    
    // Options de filtrage disponibles
    Route::get('/filter-options', [PublicEtablissementController::class, 'filterOptions']);
    
    // Établissements à proximité d'un point
    Route::post('/nearby', [PublicEtablissementController::class, 'nearby']);
    
    // Liste paginée des établissements
    Route::get('/', [PublicEtablissementController::class, 'index']);
    
    // Détails d'un établissement spécifique - DOIT ÊTRE EN DERNIER
    Route::get('/{id}', [PublicEtablissementController::class, 'show'])->where('id', '[0-9]+');
});

// ============================================
// ROUTES ADMIN (Authentifiées)
// ============================================

Route::prefix('admin')->group(function () {
    // Route publique de connexion admin
    Route::post('/login', [AdminAuthController::class, 'login']);
    
    // Routes protégées admin
    Route::middleware(['auth:sanctum', 'admin'])->group(function () {
        // Routes d'authentification admin
        Route::get('/me', [AdminAuthController::class, 'me']);
        Route::post('/logout', [AdminAuthController::class, 'logout']);
        Route::get('/dashboard', [AdminAuthController::class, 'dashboard']);
        
        // Routes de gestion des établissements (admin et superadmin)
        Route::prefix('etablissements')->group(function () {
            Route::get('/', [EtablissementController::class, 'index']);
            Route::get('/search', [EtablissementController::class, 'search']);
            Route::get('/map', [EtablissementController::class, 'map']);
            Route::get('/stats', [EtablissementController::class, 'stats']);
            Route::get('/{id}', [EtablissementController::class, 'show']);
            Route::post('/', [EtablissementController::class, 'store']);
            Route::put('/{id}', [EtablissementController::class, 'update']);
            Route::patch('/{id}', [EtablissementController::class, 'update']);
            
            // Import/Export (admin uniquement)
            Route::post('/import', [EtablissementController::class, 'import']);
            Route::get('/export', [EtablissementController::class, 'export']);
        });
        
        // Routes SuperAdmin uniquement
        Route::middleware('superadmin')->group(function () {
            // Gestion des administrateurs
            Route::get('/admins', [AdminAuthController::class, 'admins']);
            Route::post('/admins', [AdminAuthController::class, 'createAdmin']);
            
            // Suppression d'établissements (superadmin uniquement)
            Route::delete('/etablissements/{id}', [EtablissementController::class, 'destroy']);
            
            // Statistiques détaillées admin
            Route::get('/etablissements/stats/detailed', [EtablissementController::class, 'detailedStats']);
        });
    });
});

// ============================================
// ROUTES DE MAINTENANCE
// ============================================

// Route de test de santé de l'API
Route::get('/health', function () {
    return response()->json([
        'status' => 'OK',
        'timestamp' => now(),
        'version' => '1.0.0'
    ]);
});

// Route d'information sur l'API
Route::get('/info', function () {
    return response()->json([
        'name' => 'EduMap API',
        'description' => 'API pour la gestion des établissements scolaires du Togo',
        'version' => '1.0.0',
        'endpoints' => [
            'public' => [
                'GET /api/etablissements' => 'Liste des établissements',
                'GET /api/etablissements/{id}' => 'Détails d\'un établissement',
                'GET /api/etablissements/search' => 'Recherche d\'établissements',
                'GET /api/etablissements/map' => 'Données pour carte',
                'GET /api/etablissements/stats' => 'Statistiques publiques',
                'POST /api/etablissements/nearby' => 'Établissements à proximité',
            ],
            'admin' => [
                'POST /api/admin/login' => 'Connexion admin',
                'All CRUD operations on /api/admin/etablissements/*' => 'Gestion complète'
            ]
        ]
    ]);
});