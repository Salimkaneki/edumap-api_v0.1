<?php

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;
use App\Http\Controllers\EtablissementController;
use App\Http\Controllers\AdminAuthController;

// Route utilisateur standard
Route::get('/user', function (Request $request) {
    return $request->user();
})->middleware('auth:sanctum');

// Routes établissements
Route::get('/etablissements/search', [EtablissementController::class, 'search']);
Route::get('/etablissements/map', [EtablissementController::class, 'map']);
Route::get('/etablissements/{id}', [EtablissementController::class, 'show']);
Route::get('/etablissements', [EtablissementController::class, 'index']);

Route::post('/etablissements', [EtablissementController::class, 'store']);
Route::put('/etablissements/{id}', [EtablissementController::class, 'update']);
Route::delete('/etablissements/{id}', [EtablissementController::class, 'destroy']);

// Routes admin - Configuration correcte
Route::prefix('admin')->group(function () {
    // Route publique de connexion
    Route::post('/login', [AdminAuthController::class, 'login']);
    
    // Routes protégées - utiliser auth:sanctum avec middleware personnalisé
    Route::middleware(['auth:sanctum', 'admin'])->group(function () {
        Route::get('/me', [AdminAuthController::class, 'me']);
        Route::post('/logout', [AdminAuthController::class, 'logout']);
        Route::get('/dashboard', [AdminAuthController::class, 'dashboard']);
        
        // Routes SuperAdmin uniquement
        Route::middleware('superadmin')->group(function () {
            Route::get('/admins', [AdminAuthController::class, 'admins']);
            Route::post('/admins', [AdminAuthController::class, 'createAdmin']);
        });
    });
});
