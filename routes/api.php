<?php

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;

Route::get('/user', function (Request $request) {
    return $request->user();
})->middleware('auth:sanctum');


use App\Http\Controllers\EtablissementController;

// Route::middleware('auth:sanctum')->group(function () {
//     Route::post('/etablissements', [EtablissementController::class, 'store']);
//     Route::put('/etablissements/{id}', [EtablissementController::class, 'update']);
//     Route::delete('/etablissements/{id}', [EtablissementController::class, 'destroy']);
// });

// Route::get('/etablissements', [EtablissementController::class, 'index']);
// Route::get('/etablissements/{id}', [EtablissementController::class, 'show']);
// Route::get('/etablissements/search', [EtablissementController::class, 'search']);
// Route::get('/etablissements/map', [EtablissementController::class, 'map']);



Route::get('/etablissements/search', [EtablissementController::class, 'search']);
Route::get('/etablissements/map', [EtablissementController::class, 'map']);
Route::get('/etablissements/{id}', [EtablissementController::class, 'show']);
Route::get('/etablissements', [EtablissementController::class, 'index']);

Route::post('/etablissements', [EtablissementController::class, 'store']);
Route::put('/etablissements/{id}', [EtablissementController::class, 'update']);
Route::delete('/etablissements/{id}', [EtablissementController::class, 'destroy']);