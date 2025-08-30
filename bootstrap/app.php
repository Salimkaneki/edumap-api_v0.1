<?php

use Illuminate\Foundation\Application;
use Illuminate\Foundation\Configuration\Exceptions;
use Illuminate\Foundation\Configuration\Middleware;

return Application::configure(basePath: dirname(__DIR__))
    ->withRouting(
        web: __DIR__.'/../routes/web.php',
        api: __DIR__.'/../routes/api.php',
        commands: __DIR__.'/../routes/console.php',
        health: '/up',
    )
    ->withMiddleware(function (Middleware $middleware) {
        $middleware->alias([
            'api-auth' => \App\Http\Middleware\Authenticate::class,
            'admin' => \App\Http\Middleware\AdminMiddleware::class,
            'superadmin' => \App\Http\Middleware\SuperAdminMiddleware::class,
        ]);
    })
    ->withExceptions(function (Exceptions $exceptions) {
        // Gérer les erreurs d'authentification pour les API
        $exceptions->render(function (\Illuminate\Auth\AuthenticationException $exception, $request) {
            if ($request->is('api/*') || $request->expectsJson()) {
                return response()->json([
                    'error' => 'Token d\'authentification invalide ou manquant.'
                ], 401);
            }
        });
        
        // Gérer les erreurs de validation pour les API
        $exceptions->render(function (\Illuminate\Validation\ValidationException $exception, $request) {
            if ($request->is('api/*') || $request->expectsJson()) {
                return response()->json([
                    'error' => 'Données de validation invalides.',
                    'errors' => $exception->errors()
                ], 422);
            }
        });
    })->create();
