<?php

// app/Http/Controllers/PublicEtablissementController.php

namespace App\Http\Controllers;

use App\Models\Etablissement;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;

class PublicEtablissementController extends Controller
{
    /**
     * Récupérer tous les établissements avec pagination (accès public)
     */
    public function index(Request $request)
    {
        try {
            $page = $request->query('page', 1);
            $perPage = min($request->query('per_page', 10), 50); // Limite à 50 pour éviter la surcharge

            $cacheKey = 'public_etablissements_page_' . $page . '_per_page_' . $perPage;
            
            $etablissements = Cache::remember($cacheKey, 300, function () use ($perPage) {
                return Etablissement::with(['milieu', 'statut', 'systeme', 'annee'])
                    ->select([
                        'id', 'code_etablissement', 'nom_etablissement', 'region', 'prefecture',
                        'canton_village_autonome', 'ville_village_quartier', 'commune_etab',
                        'libelle_type_milieu', 'libelle_type_statut_etab', 'libelle_type_systeme',
                        'libelle_type_annee', 'latitude', 'longitude', 'tot', 'total_ense',
                        'existe_elect', 'existe_latrine', 'existe_latrine_fonct', 'acces_toute_saison', 'eau'
                    ])
                    ->paginate($perPage);
            });

            return response()->json($etablissements);
        } catch (\Exception $e) {
            Log::error("Erreur lors de la récupération des établissements: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la récupération des établissements'
            ], 500);
        }
    }

    /**
     * Afficher un établissement spécifique (accès public)
     */
    public function show($id)
    {
        try {
            Log::info("Recherche publique de l'établissement avec l'ID: " . $id);
            
            $id = (int) $id;
            
            $cacheKey = 'public_etablissement_' . $id;
            
            $etablissement = Cache::remember($cacheKey, 300, function () use ($id) {
                return Etablissement::with(['milieu', 'statut', 'systeme', 'annee'])
                    ->select([
                        'id', 'code_etablissement', 'nom_etablissement', 'region', 'prefecture',
                        'canton_village_autonome', 'ville_village_quartier', 'commune_etab',
                        'libelle_type_milieu', 'libelle_type_statut_etab', 'libelle_type_systeme',
                        'libelle_type_annee', 'latitude', 'longitude', 'tot', 'total_ense',
                        'existe_elect', 'existe_latrine', 'existe_latrine_fonct', 'acces_toute_saison', 'eau'
                    ])
                    ->find($id);
            });
            
            if (!$etablissement) {
                Log::info("Établissement non trouvé avec l'ID: " . $id);
                return response()->json(['error' => 'Établissement non trouvé'], 404);
            }
            
            return response()->json($etablissement);
        } catch (\Exception $e) {
            Log::error("Erreur lors de la recherche de l'établissement: " . $e->getMessage(), [
                'id' => $id,
                'exception' => $e->getTraceAsString()
            ]);
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la recherche de l\'établissement'
            ], 500);
        }
    }

    /**
     * Rechercher des établissements par différents critères (accès public)
     */
    public function search(Request $request)
    {
        try {
            Log::info('Recherche publique avec params:', $request->all());

            $query = Etablissement::with(['milieu', 'statut', 'systeme', 'annee'])
                ->select([
                    'id', 'code_etablissement', 'nom_etablissement', 'region', 'prefecture',
                    'canton_village_autonome', 'ville_village_quartier', 'commune_etab',
                    'libelle_type_milieu', 'libelle_type_statut_etab', 'libelle_type_systeme',
                    'libelle_type_annee', 'latitude', 'longitude', 'tot', 'total_ense',
                    'existe_elect', 'existe_latrine', 'existe_latrine_fonct', 'acces_toute_saison', 'eau'
                ]);

            // Filtres de recherche
            if ($request->filled('nom_etablissement')) {
                $query->where('nom_etablissement', 'like', '%' . $request->nom_etablissement . '%');
            }

            if ($request->filled('region')) {
                $query->where('region', $request->region);
            }

            if ($request->filled('prefecture')) {
                $query->where('prefecture', $request->prefecture);
            }

            if ($request->filled('libelle_type_milieu')) {
                $query->where('libelle_type_milieu', $request->libelle_type_milieu);
            }

            if ($request->filled('libelle_type_statut_etab')) {
                $query->where('libelle_type_statut_etab', $request->libelle_type_statut_etab);
            }

            if ($request->filled('libelle_type_systeme')) {
                $query->where('libelle_type_systeme', $request->libelle_type_systeme);
            }

            if ($request->filled('existe_elect')) {
                $query->where('existe_elect', filter_var($request->existe_elect, FILTER_VALIDATE_BOOLEAN));
            }

            if ($request->filled('existe_latrine')) {
                $query->where('existe_latrine', filter_var($request->existe_latrine, FILTER_VALIDATE_BOOLEAN));
            }

            if ($request->filled('existe_latrine_fonct')) {
                $query->where('existe_latrine_fonct', filter_var($request->existe_latrine_fonct, FILTER_VALIDATE_BOOLEAN));
            }

            if ($request->filled('acces_toute_saison')) {
                $query->where('acces_toute_saison', filter_var($request->acces_toute_saison, FILTER_VALIDATE_BOOLEAN));
            }

            if ($request->filled('eau')) {
                $query->where('eau', filter_var($request->eau, FILTER_VALIDATE_BOOLEAN));
            }

            $perPage = min($request->per_page ?? 10, 50);
            $etablissements = $query->paginate($perPage);

            return response()->json($etablissements);
        } catch (\Exception $e) {
            Log::error("Erreur lors de la recherche d'établissements: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la recherche'
            ], 500);
        }
    }

    /**
     * Récupérer les établissements avec leurs coordonnées pour la carte (accès public)
     */
    public function map()
    {
        try {
            $cacheKey = 'public_etablissements_map';
            
            $etablissements = Cache::remember($cacheKey, 600, function () {
                return Etablissement::select([
                    'id', 'nom_etablissement', 'latitude', 'longitude', 'region', 'prefecture',
                    'libelle_type_milieu', 'libelle_type_statut_etab', 'libelle_type_systeme'
                ])
                ->whereNotNull('latitude')
                ->whereNotNull('longitude')
                ->get();
            });

            return response()->json($etablissements);
        } catch (\Exception $e) {
            Log::error("Erreur lors de la récupération des données de carte: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la récupération des données de carte'
            ], 500);
        }
    }

    /**
     * Obtenir les statistiques publiques des établissements
     */
    public function stats()
    {
        try {
            $cacheKey = 'public_etablissements_stats';
            
            $stats = Cache::remember($cacheKey, 3600, function () {
                return [
                    'total_etablissements' => Etablissement::count(),
                    'par_region' => Etablissement::select('region')
                        ->selectRaw('COUNT(*) as count')
                        ->groupBy('region')
                        ->orderBy('count', 'desc')
                        ->get(),
                    'par_type_milieu' => Etablissement::select('libelle_type_milieu')
                        ->selectRaw('COUNT(*) as count')
                        ->groupBy('libelle_type_milieu')
                        ->get(),
                    'par_statut' => Etablissement::select('libelle_type_statut_etab')
                        ->selectRaw('COUNT(*) as count')
                        ->groupBy('libelle_type_statut_etab')
                        ->get(),
                    'par_systeme' => Etablissement::select('libelle_type_systeme')
                        ->selectRaw('COUNT(*) as count')
                        ->groupBy('libelle_type_systeme')
                        ->get(),
                    'total_eleves' => Etablissement::sum('tot'),
                    'total_enseignants' => Etablissement::sum('total_ense'),
                    'infrastructures' => [
                        'avec_electricite' => Etablissement::where('existe_elect', true)->count(),
                        'avec_latrines' => Etablissement::where('existe_latrine', true)->count(),
                        'avec_latrines_fonctionnelles' => Etablissement::where('existe_latrine_fonct', true)->count(),
                        'avec_eau' => Etablissement::where('eau', true)->count(),
                        'acces_toute_saison' => Etablissement::where('acces_toute_saison', true)->count(),
                    ]
                ];
            });

            return response()->json($stats);
        } catch (\Exception $e) {
            Log::error("Erreur lors du calcul des statistiques publiques: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors du calcul des statistiques'
            ], 500);
        }
    }

    /**
     * Obtenir les options de filtrage disponibles
     */
    public function filterOptions()
    {
        try {
            $cacheKey = 'public_etablissements_filter_options';
            
            $options = Cache::remember($cacheKey, 3600, function () {
                return [
                    'regions' => Etablissement::select('region')
                        ->distinct()
                        ->whereNotNull('region')
                        ->orderBy('region')
                        ->pluck('region'),
                    'prefectures' => Etablissement::select('prefecture')
                        ->distinct()
                        ->whereNotNull('prefecture')
                        ->orderBy('prefecture')
                        ->pluck('prefecture'),
                    'types_milieu' => Etablissement::select('libelle_type_milieu')
                        ->distinct()
                        ->whereNotNull('libelle_type_milieu')
                        ->orderBy('libelle_type_milieu')
                        ->pluck('libelle_type_milieu'),
                    'types_statut' => Etablissement::select('libelle_type_statut_etab')
                        ->distinct()
                        ->whereNotNull('libelle_type_statut_etab')
                        ->orderBy('libelle_type_statut_etab')
                        ->pluck('libelle_type_statut_etab'),
                    'types_systeme' => Etablissement::select('libelle_type_systeme')
                        ->distinct()
                        ->whereNotNull('libelle_type_systeme')
                        ->orderBy('libelle_type_systeme')
                        ->pluck('libelle_type_systeme'),
                ];
            });

            return response()->json($options);
        } catch (\Exception $e) {
            Log::error("Erreur lors de la récupération des options de filtrage: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la récupération des options de filtrage'
            ], 500);
        }
    }
}