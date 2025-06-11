<?php

namespace App\Http\Controllers;

use App\Models\Admin;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Hash;
use Illuminate\Validation\ValidationException;

class AdminAuthController extends Controller
{
    public function login(Request $request)
    {
        $request->validate([
            'email' => 'required|email',
            'password' => 'required|string',
        ]);

        $admin = Admin::where('email', $request->email)->first();

        if (!$admin || !Hash::check($request->password, $admin->password)) {
            throw ValidationException::withMessages([
                'email' => ['The provided credentials are incorrect.'],
            ]);
        }

        // Créer un token Sanctum
        $token = $admin->createToken('admin-token')->plainTextToken;

        return response()->json([
            'message' => 'Login successful',
            'admin' => [
                'id' => $admin->id,
                'name' => $admin->name,
                'email' => $admin->email,
                'role' => $admin->role,
                'is_superadmin' => $admin->isSuperAdmin()
            ],
            'token' => $token
        ]);
    }

    public function logout(Request $request)
    {
        // Utiliser le user authentifié via Sanctum
        $request->user()->currentAccessToken()->delete();

        return response()->json([
            'message' => 'Logged out successfully'
        ]);
    }

    public function me(Request $request)
    {
        $admin = $request->user();
        
        return response()->json([
            'admin' => [
                'id' => $admin->id,
                'name' => $admin->name,
                'email' => $admin->email,
                'role' => $admin->role,
                'is_superadmin' => $admin->isSuperAdmin()
            ]
        ]);
    }

    public function dashboard(Request $request)
    {
        $admin = $request->user();
        
        return response()->json([
            'message' => 'Welcome to admin dashboard',
            'admin' => [
                'name' => $admin->name,
                'email' => $admin->email,
                'role' => $admin->role,
                'is_superadmin' => $admin->isSuperAdmin()
            ],
            'stats' => [
                'total_admins' => Admin::count(),
                'regular_admins' => Admin::where('role', 'admin')->count(),
                'super_admins' => Admin::where('role', 'superadmin')->count(),
            ]
        ]);
    }

    public function admins()
    {
        $admins = Admin::select('id', 'name', 'email', 'role', 'created_at')->get();

        return response()->json([
            'admins' => $admins
        ]);
    }

    public function createAdmin(Request $request)
    {
        $request->validate([
            'name' => 'required|string|max:255',
            'email' => 'required|email|unique:admins',
            'password' => 'required|min:8',
            'role' => 'required|in:admin,superadmin',
        ]);

        $admin = Admin::create([
            'name' => $request->name,
            'email' => $request->email,
            'password' => Hash::make($request->password),
            'role' => $request->role,
        ]);

        return response()->json([
            'message' => 'Admin created successfully',
            'admin' => [
                'id' => $admin->id,
                'name' => $admin->name,
                'email' => $admin->email,
                'role' => $admin->role
            ]
        ]);
    }
}