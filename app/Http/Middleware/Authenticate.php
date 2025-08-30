<?php

namespace App\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Auth;

class Authenticate
{
    /**
     * Handle an incoming request.
     */
    public function handle(Request $request, Closure $next, ...$guards)
    {
        // Utiliser Sanctum par défaut si aucun guard spécifié
        if (empty($guards)) {
            $guards = ['sanctum'];
        }

        // Vérifier l'authentification
        foreach ($guards as $guard) {
            if (Auth::guard($guard)->check()) {
                return $next($request);
            }
        }

        // Si non authentifié, retourner une erreur JSON 401
        return response()->json([
            'error' => 'Token d\'authentification invalide ou manquant.',
            'message' => 'Vous devez être connecté pour accéder à cette ressource.'
        ], 401);
    }
}
