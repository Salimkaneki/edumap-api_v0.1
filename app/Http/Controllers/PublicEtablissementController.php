<?php

// app/Http/Controllers/PublicEtablissementController.php

namespace App\Http\Controllers;

use App\Models\Etablissement;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\DB;

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
                return Etablissement::with(['localisation', 'milieu', 'statut', 'systeme', 'annee', 'effectif', 'infrastructure', 'equipement'])
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
                return Etablissement::with(['localisation', 'milieu', 'statut', 'systeme', 'annee', 'effectif', 'infrastructure', 'equipement'])
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

            $query = Etablissement::with(['localisation', 'milieu', 'statut', 'systeme', 'annee', 'effectif', 'infrastructure', 'equipement']);

            // Filtres de recherche
            if ($request->filled('nom_etablissement')) {
                $query->where('nom_etablissement', 'like', '%' . $request->nom_etablissement . '%');
            }

            if ($request->filled('region')) {
                $query->whereHas('localisation', function($q) use ($request) {
                    $q->where('region', $request->region);
                });
            }

            if ($request->filled('prefecture')) {
                $query->whereHas('localisation', function($q) use ($request) {
                    $q->where('prefecture', $request->prefecture);
                });
            }

            if ($request->filled('libelle_type_milieu')) {
                $query->whereHas('milieu', function($q) use ($request) {
                    $q->where('libelle_type_milieu', $request->libelle_type_milieu);
                });
            }

            if ($request->filled('libelle_type_statut_etab')) {
                $query->whereHas('statut', function($q) use ($request) {
                    $q->where('libelle_type_statut_etab', $request->libelle_type_statut_etab);
                });
            }

            if ($request->filled('libelle_type_systeme')) {
                $query->whereHas('systeme', function($q) use ($request) {
                    $q->where('libelle_type_systeme', $request->libelle_type_systeme);
                });
            }

            if ($request->filled('existe_elect')) {
                $query->whereHas('equipement', function($q) use ($request) {
                    $q->where('existe_elect', filter_var($request->existe_elect, FILTER_VALIDATE_BOOLEAN));
                });
            }

            if ($request->filled('existe_latrine')) {
                $query->whereHas('equipement', function($q) use ($request) {
                    $q->where('existe_latrine', filter_var($request->existe_latrine, FILTER_VALIDATE_BOOLEAN));
                });
            }

            if ($request->filled('existe_latrine_fonct')) {
                $query->whereHas('equipement', function($q) use ($request) {
                    $q->where('existe_latrine_fonct', filter_var($request->existe_latrine_fonct, FILTER_VALIDATE_BOOLEAN));
                });
            }

            if ($request->filled('acces_toute_saison')) {
                $query->whereHas('equipement', function($q) use ($request) {
                    $q->where('acces_toute_saison', filter_var($request->acces_toute_saison, FILTER_VALIDATE_BOOLEAN));
                });
            }

            if ($request->filled('eau')) {
                $query->whereHas('equipement', function($q) use ($request) {
                    $q->where('eau', filter_var($request->eau, FILTER_VALIDATE_BOOLEAN));
                });
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
                return Etablissement::with(['localisation', 'milieu', 'statut', 'systeme'])
                    ->select('id', 'nom_etablissement', 'latitude', 'longitude', 'localisation_id', 'milieu_id', 'statut_id', 'systeme_id')
                    ->whereNotNull('latitude')
                    ->whereNotNull('longitude')
                    ->limit(10) // Limite à 1000 établissements pour la carte
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
                    'par_region' => DB::table('etablissements')
                        ->join('localisations', 'etablissements.localisation_id', '=', 'localisations.id')
                        ->select('localisations.region')
                        ->selectRaw('COUNT(*) as count')
                        ->groupBy('localisations.region')
                        ->orderBy('count', 'desc')
                        ->get(),
                    'par_type_milieu' => DB::table('etablissements')
                        ->join('milieux', 'etablissements.milieu_id', '=', 'milieux.id')
                        ->select('milieux.libelle_type_milieu')
                        ->selectRaw('COUNT(*) as count')
                        ->groupBy('milieux.libelle_type_milieu')
                        ->get(),
                    'par_statut' => DB::table('etablissements')
                        ->join('statuts', 'etablissements.statut_id', '=', 'statuts.id')
                        ->select('statuts.libelle_type_statut_etab')
                        ->selectRaw('COUNT(*) as count')
                        ->groupBy('statuts.libelle_type_statut_etab')
                        ->get(),
                    'par_systeme' => DB::table('etablissements')
                        ->join('systemes', 'etablissements.systeme_id', '=', 'systemes.id')
                        ->select('systemes.libelle_type_systeme')
                        ->selectRaw('COUNT(*) as count')
                        ->groupBy('systemes.libelle_type_systeme')
                        ->get(),
                    'total_eleves' => DB::table('effectifs')->sum('tot'),
                    'total_enseignants' => DB::table('effectifs')->sum('total_ense'),
                    'infrastructures' => [
                        'avec_electricite' => DB::table('equipements_etablissement')->where('existe_elect', true)->count(),
                        'avec_latrines' => DB::table('equipements_etablissement')->where('existe_latrine', true)->count(),
                        'avec_latrines_fonctionnelles' => DB::table('equipements_etablissement')->where('existe_latrine_fonct', true)->count(),
                        'avec_eau' => DB::table('equipements_etablissement')->where('eau', true)->count(),
                        'acces_toute_saison' => DB::table('equipements_etablissement')->where('acces_toute_saison', true)->count(),
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
                    'regions' => DB::table('localisations')
                        ->distinct()
                        ->whereNotNull('region')
                        ->orderBy('region')
                        ->pluck('region'),
                    'prefectures' => DB::table('localisations')
                        ->distinct()
                        ->whereNotNull('prefecture')
                        ->orderBy('prefecture')
                        ->pluck('prefecture'),
                    'types_milieu' => DB::table('milieux')
                        ->distinct()
                        ->whereNotNull('libelle_type_milieu')
                        ->orderBy('libelle_type_milieu')
                        ->pluck('libelle_type_milieu'),
                    'types_statut' => DB::table('statuts')
                        ->distinct()
                        ->whereNotNull('libelle_type_statut_etab')
                        ->orderBy('libelle_type_statut_etab')
                        ->pluck('libelle_type_statut_etab'),
                    'types_systeme' => DB::table('systemes')
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

    /**
     * Établissements à proximité d'un point
     */
    public function nearby(Request $request)
    {
        try {
            $request->validate([
                'lat' => 'required|numeric|between:-90,90',
                'lng' => 'required|numeric|between:-180,180',
                'radius' => 'nullable|numeric|min:0.1|max:100'
            ]);

            $latitude = $request->lat;
            $longitude = $request->lng;
            $radius = $request->radius ?? 10; // 10km par défaut

            // Utiliser la formule de Haversine pour calculer la distance
            $etablissements = Etablissement::with(['localisation', 'milieu', 'statut', 'systeme', 'effectif', 'infrastructure', 'equipement'])
                ->selectRaw("*, (
                    6371 * acos(
                        cos(radians(?)) * 
                        cos(radians(latitude)) * 
                        cos(radians(longitude) - radians(?)) + 
                        sin(radians(?)) * 
                        sin(radians(latitude))
                    )
                ) AS distance", [$latitude, $longitude, $latitude])
                ->whereNotNull('latitude')
                ->whereNotNull('longitude')
                ->having('distance', '<', $radius)
                ->orderBy('distance')
                ->limit(50) // Limiter à 50 établissements max
                ->get();

            return response()->json([
                'center' => [
                    'lat' => $latitude,
                    'lng' => $longitude
                ],
                'radius' => $radius,
                'count' => $etablissements->count(),
                'etablissements' => $etablissements
            ]);

        } catch (\Illuminate\Validation\ValidationException $e) {
            return response()->json([
                'error' => 'Données invalides',
                'details' => $e->errors()
            ], 422);
        } catch (\Exception $e) {
            Log::error("Erreur lors de la recherche à proximité: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la recherche à proximité'
            ], 500);
        }
    }
}