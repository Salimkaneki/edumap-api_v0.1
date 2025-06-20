<?php

namespace App\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

class SuperAdminMiddleware
{
    /**
     * Handle an incoming request.
     *
     * @param  \Closure(\Illuminate\Http\Request): (\Symfony\Component\HttpFoundation\Response)  $next
     */
    public function handle(Request $request, Closure $next): Response
    {
        $user = $request->user();
        
        if (!$user instanceof \App\Models\Admin || !$user->isSuperAdmin()) {
            return response()->json([
                'message' => 'Unauthorized. Super admin access required.'
            ], 403);
        }

        return $next($request);
    }
}
