<?php

namespace App\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

class SanctumApiMiddleware
{
    /**
     * Handle an incoming request.
     *
     * @param  \Closure(\Illuminate\Http\Request): (\Symfony\Component\HttpFoundation\Response)  $next
     */
    public function handle(Request $request, Closure $next): Response
    {
         // Spécifier le guard 'admin' au lieu du guard par défaut
        if (!$request->user('admin')) {
            return response()->json(['message' => 'Unauthenticated'], 401);
        }
        return $next($request);
    }
}
