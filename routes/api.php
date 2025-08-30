<?php

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;
use App\Http\Controllers\EtablissementController;
use App\Http\Controllers\AdminAuthController;
use App\Http\Controllers\PublicEtablissementController;


// Route utilisateur standard
Route::get('/user', function (Request $request) {
    return $request->user();
})->middleware('auth:sanctum');

// Routes établissements publiques (consultation)
Route::get('/etablissements/search', [EtablissementController::class, 'search']);
Route::get('/etablissements/map', [EtablissementController::class, 'map']);
Route::get('/etablissements/{id}', [EtablissementController::class, 'show']);
Route::get('/etablissements', [EtablissementController::class, 'index']);

// Routes admin - Configuration correcte
Route::prefix('admin')->group(function () {
    // Route publique de connexion admin
    Route::post('/login', [AdminAuthController::class, 'login']);
    
    // Routes protégées admin - utiliser auth:sanctum avec middleware personnalisé
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
        });
        
        // Routes SuperAdmin uniquement
        Route::middleware('superadmin')->group(function () {
            // Gestion des administrateurs
            Route::get('/admins', [AdminAuthController::class, 'admins']);
            Route::post('/admins', [AdminAuthController::class, 'createAdmin']);
            
            // Suppression d'établissements (superadmin uniquement)
            Route::delete('/etablissements/{id}', [EtablissementController::class, 'destroy']);
        });
    });
});

// Routes établissements protégées (création, modification, suppression)
// Ces routes sont dupliquées dans le groupe admin pour plus de clarté
Route::middleware(['auth:sanctum', 'admin'])->group(function () {
    Route::post('/etablissements', [EtablissementController::class, 'store']);
    Route::put('/etablissements/{id}', [EtablissementController::class, 'update']);
    Route::patch('/etablissements/{id}', [EtablissementController::class, 'update']);
});

// Suppression réservée aux superadmins
Route::middleware(['auth:sanctum', 'admin', 'superadmin'])->group(function () {
    Route::delete('/etablissements/{id}', [EtablissementController::class, 'destroy']);
});










// Routes publiques pour les établissements
// Route::prefix('etablissements')->group(function () {
//     // Liste paginée des établissements
//     Route::get('/', [PublicEtablissementController::class, 'index']);
    
//     // Détails d'un établissement spécifique
//     Route::get('/{id}', [PublicEtablissementController::class, 'show']);
    
//     // Recherche d'établissements avec filtres
//     Route::get('/search', [PublicEtablissementController::class, 'search']);
    
//     // Données pour la carte (coordonnées géographiques)
//     Route::get('/map', [PublicEtablissementController::class, 'map']);
    
//     // Statistiques globales
//     Route::get('/stats', [PublicEtablissementController::class, 'stats']);
    
//     // Options de filtrage disponibles
//     Route::get('/filter-options', [PublicEtablissementController::class, 'filterOptions']);
// });




// use App\Http\Controllers\AdminEtablissementController;

// // Routes admin pour la gestion des établissements
// Route::prefix('admin/etablissements')->middleware(['auth:api', 'admin'])->group(function () {
//     // Liste paginée des établissements (admin)
//     Route::get('/', [AdminEtablissementController::class, 'index']);
    
//     // Détails d'un établissement spécifique (admin)
//     Route::get('/{id}', [AdminEtablissementController::class, 'show']);
    
//     // Recherche avancée d'établissements (admin)
//     Route::get('/search', [AdminEtablissementController::class, 'search']);
    
//     // Création d'un nouvel établissement (admin)
//     Route::post('/', [AdminEtablissementController::class, 'store']);
    
//     // Modification d'un établissement (admin)
//     Route::put('/{id}', [AdminEtablissementController::class, 'update']);
//     Route::patch('/{id}', [AdminEtablissementController::class, 'update']);
    
//     // Suppression d'un établissement (superadmin uniquement)
//     Route::delete('/{id}', [AdminEtablissementController::class, 'destroy'])->middleware('superadmin');
    
//     // Statistiques détaillées (admin)
//     Route::get('/stats', [AdminEtablissementController::class, 'stats']);
    
//     // Données pour la carte (admin)
//     Route::get('/map', [AdminEtablissementController::class, 'map']);
    
//     // Options de filtrage (admin)
//     Route::get('/filter-options', [AdminEtablissementController::class, 'filterOptions']);
    
//     // Export des données (admin)
//     Route::get('/export', [AdminEtablissementController::class, 'export']);
    
//     // Historique des modifications (admin)
//     Route::get('/{id}/history', [AdminEtablissementController::class, 'history']);
// });