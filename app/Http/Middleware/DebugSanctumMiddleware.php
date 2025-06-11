<?php

namespace App\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

class DebugSanctumMiddleware
{
    public function handle(Request $request, Closure $next): Response
    {
        \Log::info('=== DEBUG SANCTUM START ===');
        
        // Debug headers
        \Log::info('Request headers', [
            'authorization' => $request->header('Authorization'),
            'accept' => $request->header('Accept'),
            'content-type' => $request->header('Content-Type'),
        ]);
        
        // Debug token
        $token = $request->bearerToken();
        \Log::info('Bearer token', ['token' => $token ? substr($token, 0, 20) . '...' : 'no token']);
        
        // Tenter d'authentifier manuellement
        try {
            if ($token) {
                $accessToken = \Laravel\Sanctum\PersonalAccessToken::findToken($token);
                \Log::info('Token found', [
                    'token_exists' => $accessToken ? 'yes' : 'no',
                    'tokenable_type' => $accessToken?->tokenable_type,
                    'tokenable_id' => $accessToken?->tokenable_id,
                ]);
                
                if ($accessToken) {
                    $user = $accessToken->tokenable;
                    \Log::info('User from token', [
                        'user_type' => get_class($user),
                        'user_id' => $user->id,
                        'user_email' => $user->email ?? 'no email',
                    ]);
                }
            }
        } catch (\Exception $e) {
            \Log::error('Token debug error', ['error' => $e->getMessage()]);
        }
        
        // Test request->user()
        try {
            $user = $request->user();
            \Log::info('Request user', [
                'user' => $user ? get_class($user) : 'null',
                'user_id' => $user?->id,
            ]);
        } catch (\Exception $e) {
            \Log::error('Request user error', ['error' => $e->getMessage()]);
        }
        
        \Log::info('=== DEBUG SANCTUM END ===');
        
        return $next($request);
    }
}