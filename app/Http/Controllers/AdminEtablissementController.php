<?php

// app/Http/Controllers/AdminEtablissementController.php

namespace App\Http\Controllers;

use App\Models\Etablissement;
use App\Models\Localisation;
use App\Models\Milieu;
use App\Models\Statut;
use App\Models\Systeme;
use App\Models\Annee;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Validator;

class AdminEtablissementController extends Controller
{
    /**
     * Récupérer tous les établissements avec pagination (accès admin)
     */
    public function index(Request $request)
    {
        try {
            $admin = $request->user();
            Log::info("Admin accès à la liste des établissements:", ['admin_id' => $admin->id]);

            $page = $request->query('page', 1);
            $perPage = min($request->query('per_page', 15), 100);

            $cacheKey = 'admin_etablissements_page_' . $page . '_per_page_' . $perPage;
            
            $etablissements = Cache::remember($cacheKey, 300, function () use ($perPage) {
                return Etablissement::with(['milieu', 'statut', 'systeme', 'annee'])
                    ->latest()
                    ->paginate($perPage);
            });

            return response()->json($etablissements);
        } catch (\Exception $e) {
            Log::error("Erreur admin lors de la récupération des établissements: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la récupération des établissements'
            ], 500);
        }
    }

    /**
     * Afficher un établissement spécifique (accès admin)
     */
    public function show($id)
    {
        try {
            $admin = $request->user();
            Log::info("Admin recherche établissement:", ['admin_id' => $admin->id, 'etablissement_id' => $id]);
            
            $id = (int) $id;
            
            $etablissement = Etablissement::with(['milieu', 'statut', 'systeme', 'annee'])->find($id);
            
            if (!$etablissement) {
                Log::warning("Établissement non trouvé (admin):", ['id' => $id]);
                return response()->json(['error' => 'Établissement non trouvé'], 404);
            }
            
            return response()->json($etablissement);
        } catch (\Exception $e) {
            Log::error("Erreur admin lors de la recherche de l'établissement: " . $e->getMessage(), [
                'id' => $id,
                'exception' => $e->getTraceAsString()
            ]);
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la recherche de l\'établissement'
            ], 500);
        }
    }

    /**
     * Rechercher des établissements par différents critères (accès admin)
     */
    public function search(Request $request)
    {
        try {
            $admin = $request->user();
            Log::info('Recherche admin avec params:', ['admin_id' => $admin->id, 'params' => $request->all()]);

            $query = Etablissement::with(['milieu', 'statut', 'systeme', 'annee']);

            // Filtres de recherche
            if ($request->filled('nom_etablissement')) {
                $query->where('nom_etablissement', 'like', '%' . $request->nom_etablissement . '%');
            }

            if ($request->filled('code_etablissement')) {
                $query->where('code_etablissement', 'like', '%' . $request->code_etablissement . '%');
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

            $perPage = min($request->per_page ?? 15, 100);
            $etablissements = $query->latest()->paginate($perPage);

            return response()->json($etablissements);
        } catch (\Exception $e) {
            Log::error("Erreur admin lors de la recherche d'établissements: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la recherche'
            ], 500);
        }
    }

    /**
     * Ajouter un établissement (admin)
     */
    public function store(Request $request)
    {
        try {
            $admin = $request->user();
            Log::info("Admin création établissement:", ['admin_id' => $admin->id, 'admin_name' => $admin->name]);

            $validator = Validator::make($request->all(), [
                'code_etablissement' => 'required|string|unique:etablissements',
                'nom_etablissement' => 'required|string|max:255',
                'region' => 'required|string|max:100',
                'prefecture' => 'required|string|max:100',
                'canton_village_autonome' => 'required|string|max:100',
                'ville_village_quartier' => 'required|string|max:100',
                'libelle_type_milieu' => 'required|string|in:Rural,Urbain',
                'libelle_type_statut_etab' => 'required|string|in:Public,Privé',
                'libelle_type_systeme' => 'required|string|in:SECONDAIRE I,SECONDAIRE II,PRIMAIRE,PRESCOLAIRE',
                'existe_elect' => 'boolean',
                'existe_latrine' => 'boolean',
                'existe_latrine_fonct' => 'boolean',
                'acces_toute_saison' => 'boolean',
                'eau' => 'boolean',
                'latitude' => 'required|numeric|between:-90,90',
                'longitude' => 'required|numeric|between:-180,180',
                'sommedenb_eff_g' => 'integer|min:0',
                'sommedenb_eff_f' => 'integer|min:0',
                'tot' => 'integer|min:0',
                'sommedenb_ens_h' => 'integer|min:0',
                'sommedenb_ens_f' => 'integer|min:0',
                'total_ense' => 'integer|min:0',
                'sommedenb_salles_classes_dur' => 'integer|min:0',
                'sommedenb_salles_classes_banco' => 'integer|min:0',
                'sommedenb_salles_classes_autre' => 'integer|min:0',
                'libelle_type_annee' => 'nullable|string|max:50',
                'commune_etab' => 'nullable|string|max:100',
            ]);

            if ($validator->fails()) {
                Log::warning("Validation échouée pour création établissement:", $validator->errors()->toArray());
                return response()->json([
                    'error' => 'Données invalides',
                    'details' => $validator->errors()
                ], 422);
            }

            $data = $request->all();
            
            // Mapper les libellés vers les IDs
            $milieu = Milieu::where('libelle_type_milieu', $data['libelle_type_milieu'])->first();
            if (!$milieu) {
                return response()->json([
                    'error' => 'Type de milieu invalide: ' . $data['libelle_type_milieu']
                ], 422);
            }
            $data['milieu_id'] = $milieu->id;
            
            $statut = Statut::where('libelle_type_statut_etab', $data['libelle_type_statut_etab'])->first();
            if (!$statut) {
                return response()->json([
                    'error' => 'Type de statut invalide: ' . $data['libelle_type_statut_etab']
                ], 422);
            }
            $data['statut_id'] = $statut->id;
            
            $systeme = Systeme::where('libelle_type_systeme', $data['libelle_type_systeme'])->first();
            if (!$systeme) {
                return response()->json([
                    'error' => 'Type de système invalide: ' . $data['libelle_type_systeme']
                ], 422);
            }
            $data['systeme_id'] = $systeme->id;
            
            if (!empty($data['libelle_type_annee'])) {
                $annee = Annee::where('libelle_type_annee', $data['libelle_type_annee'])->first();
                if ($annee) {
                    $data['annee_id'] = $annee->id;
                }
            }

            // Calculer automatiquement certains totaux si pas fournis
            if (!isset($data['tot']) || $data['tot'] === null) {
                $data['tot'] = ($data['sommedenb_eff_g'] ?? 0) + ($data['sommedenb_eff_f'] ?? 0);
            }
            
            if (!isset($data['total_ense']) || $data['total_ense'] === null) {
                $data['total_ense'] = ($data['sommedenb_ens_h'] ?? 0) + ($data['sommedenb_ens_f'] ?? 0);
            }

            // Ajouter l'ID de l'admin créateur
            $data['created_by'] = $admin->id;

            // Supprimer les libellés pour éviter les erreurs d'insertion
            unset($data['libelle_type_milieu']);
            unset($data['libelle_type_statut_etab']);
            unset($data['libelle_type_systeme']);
            unset($data['libelle_type_annee']);

            $etablissement = Etablissement::create($data);

            // Vider le cache
            Cache::flush();

            Log::info("Établissement créé avec succès:", [
                'etablissement_id' => $etablissement->id,
                'nom' => $etablissement->nom_etablissement,
                'created_by' => $admin->name
            ]);

            $etablissement->refresh();
            return response()->json([
                'message' => 'Établissement créé avec succès',
                'data' => $etablissement->load(['milieu', 'statut', 'systeme', 'annee'])
            ], 201);

        } catch (\Exception $e) {
            Log::error("Erreur lors de la création de l'établissement: " . $e->getMessage(), [
                'exception' => $e->getTraceAsString(),
                'request_data' => $request->all()
            ]);

            return response()->json([
                'error' => 'Une erreur est survenue lors de la création de l\'établissement',
                'message' => $e->getMessage()
            ], 500);
        }
    }

    /**
     * Modifier un établissement (admin)
     */
    public function update(Request $request, $id)
    {
        try {
            $admin = $request->user();
            $etablissement = Etablissement::findOrFail($id);

            Log::info("Admin modification établissement:", [
                'admin_id' => $admin->id,
                'admin_name' => $admin->name,
                'etablissement_id' => $id
            ]);

            $validator = Validator::make($request->all(), [
                'code_etablissement' => 'sometimes|string|unique:etablissements,code_etablissement,' . $id,
                'nom_etablissement' => 'sometimes|string|max:255',
                'region' => 'sometimes|string|max:100',
                'prefecture' => 'sometimes|string|max:100',
                'canton_village_autonome' => 'sometimes|string|max:100',
                'ville_village_quartier' => 'sometimes|string|max:100',
                'libelle_type_milieu' => 'sometimes|string|in:Rural,Urbain',
                'libelle_type_statut_etab' => 'sometimes|string|in:Public,Privé',
                'libelle_type_systeme' => 'sometimes|string|in:SECONDAIRE I,SECONDAIRE II,PRIMAIRE,PRESCOLAIRE',
                'existe_elect' => 'boolean',
                'existe_latrine' => 'boolean',
                'existe_latrine_fonct' => 'boolean',
                'acces_toute_saison' => 'boolean',
                'eau' => 'boolean',
                'latitude' => 'sometimes|numeric|between:-90,90',
                'longitude' => 'sometimes|numeric|between:-180,180',
                'sommedenb_eff_g' => 'integer|min:0',
                'sommedenb_eff_f' => 'integer|min:0',
                'tot' => 'integer|min:0',
                'sommedenb_ens_h' => 'integer|min:0',
                'sommedenb_ens_f' => 'integer|min:0',
                'total_ense' => 'integer|min:0',
                'sommedenb_salles_classes_dur' => 'integer|min:0',
                'sommedenb_salles_classes_banco' => 'integer|min:0',
                'sommedenb_salles_classes_autre' => 'integer|min:0',
                'libelle_type_annee' => 'nullable|string|max:50',
                'commune_etab' => 'nullable|string|max:100',
            ]);

            if ($validator->fails()) {
                return response()->json([
                    'error' => 'Données invalides',
                    'details' => $validator->errors()
                ], 422);
            }

            $data = $request->all();
            
            // Mapper les libellés vers les IDs pour la mise à jour
            if (isset($data['libelle_type_milieu'])) {
                $milieu = Milieu::where('libelle_type_milieu', $data['libelle_type_milieu'])->first();
                if (!$milieu) {
                    return response()->json([
                        'error' => 'Type de milieu invalide: ' . $data['libelle_type_milieu']
                    ], 422);
                }
                $data['milieu_id'] = $milieu->id;
                unset($data['libelle_type_milieu']);
            }
            
            if (isset($data['libelle_type_statut_etab'])) {
                $statut = Statut::where('libelle_type_statut_etab', $data['libelle_type_statut_etab'])->first();
                if (!$statut) {
                    return response()->json([
                        'error' => 'Type de statut invalide: ' . $data['libelle_type_statut_etab']
                    ], 422);
                }
                $data['statut_id'] = $statut->id;
                unset($data['libelle_type_statut_etab']);
            }
            
            if (isset($data['libelle_type_systeme'])) {
                $systeme = Systeme::where('libelle_type_systeme', $data['libelle_type_systeme'])->first();
                if (!$systeme) {
                    return response()->json([
                        'error' => 'Type de système invalide: ' . $data['libelle_type_systeme']
                    ], 422);
                }
                $data['systeme_id'] = $systeme->id;
                unset($data['libelle_type_systeme']);
            }
            
            if (isset($data['libelle_type_annee'])) {
                if (!empty($data['libelle_type_annee'])) {
                    $annee = Annee::where('libelle_type_annee', $data['libelle_type_annee'])->first();
                    if ($annee) {
                        $data['annee_id'] = $annee->id;
                    }
                }
                unset($data['libelle_type_annee']);
            }
            
            // Recalculer les totaux si les effectifs sont modifiés
            if (isset($data['sommedenb_eff_g']) || isset($data['sommedenb_eff_f'])) {
                $data['tot'] = ($data['sommedenb_eff_g'] ?? $etablissement->sommedenb_eff_g ?? 0) + 
                              ($data['sommedenb_eff_f'] ?? $etablissement->sommedenb_eff_f ?? 0);
            }
            
            if (isset($data['sommedenb_ens_h']) || isset($data['sommedenb_ens_f'])) {
                $data['total_ense'] = ($data['sommedenb_ens_h'] ?? $etablissement->sommedenb_ens_h ?? 0) + 
                                     ($data['sommedenb_ens_f'] ?? $etablissement->sommedenb_ens_f ?? 0);
            }

            // Ajouter l'ID de l'admin modificateur
            $data['updated_by'] = $admin->id;

            $etablissement->update($data);

            // Vider le cache
            Cache::flush();

            Log::info("Établissement modifié avec succès:", [
                'etablissement_id' => $etablissement->id,
                'modified_by' => $admin->name
            ]);

            return response()->json([
                'message' => 'Établissement modifié avec succès',
                'data' => $etablissement->load(['milieu', 'statut', 'systeme', 'annee'])
            ]);

        } catch (\Exception $e) {
            Log::error("Erreur lors de la modification de l'établissement: " . $e->getMessage(), [
                'etablissement_id' => $id,
                'exception' => $e->getTraceAsString()
            ]);

            return response()->json([
                'error' => 'Une erreur est survenue lors de la modification de l\'établissement',
                'message' => $e->getMessage()
            ], 500);
        }
    }

    /**
     * Supprimer un établissement (superadmin uniquement)
     */
    public function destroy(Request $request, $id)
    {
        try {
            $admin = $request->user();
            
            // Vérifier si c'est un superadmin
            if (!$admin->isSuperAdmin()) {
                Log::warning("Tentative de suppression par un admin non-superadmin:", [
                    'admin_id' => $admin->id,
                    'admin_role' => $admin->role
                ]);
                
                return response()->json([
                    'error' => 'Seuls les super administrateurs peuvent supprimer des établissements'
                ], 403);
            }

            $etablissement = Etablissement::findOrFail($id);
            
            Log::info("Suppression établissement par superadmin:", [
                'etablissement_id' => $id,
                'etablissement_nom' => $etablissement->nom_etablissement,
                'deleted_by' => $admin->name
            ]);

            $etablissement->delete();

            // Vider le cache
            Cache::flush();

            return response()->json([
                'message' => 'Établissement supprimé avec succès'
            ], 200);

        } catch (\Exception $e) {
            Log::error("Erreur lors de la suppression de l'établissement: " . $e->getMessage(), [
                'etablissement_id' => $id,
                'exception' => $e->getTraceAsString()
            ]);

            return response()->json([
                'error' => 'Une erreur est survenue lors de la suppression de l\'établissement',
                'message' => $e->getMessage()
            ], 500);
        }
    }

    /**
     * Obtenir les statistiques détaillées des établissements (admin)
     */
    public function stats(Request $request)
    {
        try {
            $admin = $request->user();
            Log::info("Admin consultation statistiques:", ['admin_id' => $admin->id]);

            $cacheKey = 'admin_etablissements_stats';
            
            $stats = Cache::remember($cacheKey, 1800, function () {
                return [
                    'total_etablissements' => Etablissement::count(),
                    'repartition_geographique' => [
                        'par_region' => Etablissement::select('region')
                            ->selectRaw('COUNT(*) as count')
                            ->groupBy('region')
                            ->orderBy('count', 'desc')
                            ->get(),
                        'par_prefecture' => Etablissement::select('prefecture')
                            ->selectRaw('COUNT(*) as count')
                            ->groupBy('prefecture')
                            ->orderBy('count', 'desc')
                            ->limit(10)
                            ->get(),
                    ],
                    'caracteristiques' => [
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
                    ],
                    'effectifs' => [
                        'total_eleves' => Etablissement::sum('tot'),
                        'total_eleves_garcons' => Etablissement::sum('sommedenb_eff_g'),
                        'total_eleves_filles' => Etablissement::sum('sommedenb_eff_f'),
                        'total_enseignants' => Etablissement::sum('total_ense'),
                        'total_enseignants_hommes' => Etablissement::sum('sommedenb_ens_h'),
                        'total_enseignants_femmes' => Etablissement::sum('sommedenb_ens_f'),
                        'ratio_eleves_enseignants' => Etablissement::sum('tot') > 0 && Etablissement::sum('total_ense') > 0 
                            ? round(Etablissement::sum('tot') / Etablissement::sum('total_ense'), 2) 
                            : 0,
                    ],
                    'infrastructures' => [
                        'avec_electricite' => Etablissement::where('existe_elect', true)->count(),
                        'avec_latrines' => Etablissement::where('existe_latrine', true)->count(),
                        'avec_latrines_fonctionnelles' => Etablissement::where('existe_latrine_fonct', true)->count(),
                        'avec_eau' => Etablissement::where('eau', true)->count(),
                        'acces_toute_saison' => Etablissement::where('acces_toute_saison', true)->count(),
                        'pourcentages' => [
                            'electricite' => round((Etablissement::where('existe_elect', true)->count() / Etablissement::count()) * 100, 2),
                            'latrines' => round((Etablissement::where('existe_latrine', true)->count() / Etablissement::count()) * 100, 2),
                            'eau' => round((Etablissement::where('eau', true)->count() / Etablissement::count()) * 100, 2),
                            'acces_toute_saison' => round((Etablissement::where('acces_toute_saison', true)->count() / Etablissement::count()) * 100, 2),
                        ]
                    ],
                    'salles_classes' => [
                        'total_salles_dur' => Etablissement::sum('sommedenb_salles_classes_dur'),
                        'total_salles_banco' => Etablissement::sum('sommedenb_salles_classes_banco'),
                        'total_salles_autre' => Etablissement::sum('sommedenb_salles_classes_autre'),
                    ],
                    'derniere_mise_a_jour' => now()->format('Y-m-d H:i:s'),
                ];
            });

            return response()->json($stats);

        } catch (\Exception $e) {
            Log::error("Erreur lors du calcul des statistiques admin: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors du calcul des statistiques'
            ], 500);
        }
    }

    /**
     * Obtenir les données pour la carte admin
     */
    public function map(Request $request)
    {
        try {
            $admin = $request->user();
            Log::info("Admin consultation carte:", ['admin_id' => $admin->id]);

            $cacheKey = 'admin_etablissements_map';
            
            $etablissements = Cache::remember($cacheKey, 600, function () {
                return Etablissement::select([
                    'id', 'nom_etablissement', 'code_etablissement', 'latitude', 'longitude', 
                    'region', 'prefecture', 'canton_village_autonome', 'ville_village_quartier',
                    'libelle_type_milieu', 'libelle_type_statut_etab', 'libelle_type_systeme',
                    'tot', 'total_ense', 'existe_elect', 'existe_latrine', 'existe_latrine_fonct',
                    'acces_toute_saison', 'eau', 'created_at', 'updated_at'
                ])
                ->whereNotNull('latitude')
                ->whereNotNull('longitude')
                ->get();
            });

            return response()->json($etablissements);
        } catch (\Exception $e) {
            Log::error("Erreur lors de la récupération des données de carte admin: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la récupération des données de carte'
            ], 500);
        }
    }

    /**
     * Obtenir les options de filtrage pour l'interface admin
     */
    public function filterOptions(Request $request)
    {
        try {
            $admin = $request->user();
            
            $cacheKey = 'admin_etablissements_filter_options';
            
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
                    'cantons' => Etablissement::select('canton_village_autonome')
                        ->distinct()
                        ->whereNotNull('canton_village_autonome')
                        ->orderBy('canton_village_autonome')
                        ->pluck('canton_village_autonome'),
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
                    'annees' => Etablissement::select('libelle_type_annee')
                        ->distinct()
                        ->whereNotNull('libelle_type_annee')
                        ->orderBy('libelle_type_annee')
                        ->pluck('libelle_type_annee'),
                ];
            });

            return response()->json($options);
        } catch (\Exception $e) {
            Log::error("Erreur lors de la récupération des options de filtrage admin: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la récupération des options de filtrage'
            ], 500);
        }
    }

    /**
     * Exporter les données des établissements (admin)
     */
    public function export(Request $request)
    {
        try {
            $admin = $request->user();
            Log::info("Admin export données:", ['admin_id' => $admin->id]);

            $format = $request->query('format', 'csv');
            $filters = $request->query('filters', []);

            $query = Etablissement::with(['milieu', 'statut', 'systeme', 'annee']);

            // Appliquer les filtres si fournis
            if (!empty($filters)) {
                foreach ($filters as $key => $value) {
                    if ($value !== null && $value !== '') {
                        $query->where($key, $value);
                    }
                }
            }

            $etablissements = $query->get();

            // Ici vous pouvez ajouter la logique d'export selon le format
            // Pour l'exemple, on retourne juste les données JSON
            return response()->json([
                'message' => 'Export préparé',
                'format' => $format,
                'count' => $etablissements->count(),
                'data' => $etablissements
            ]);

        } catch (\Exception $e) {
            Log::error("Erreur lors de l'export admin: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de l\'export'
            ], 500);
        }
    }

    /**
     * Obtenir le journal des modifications d'un établissement (admin)
     */
    public function history(Request $request, $id)
    {
        try {
            $admin = $request->user();
            
            $etablissement = Etablissement::findOrFail($id);
            
            Log::info("Admin consultation historique:", [
                'admin_id' => $admin->id,
                'etablissement_id' => $id
            ]);

            // Ici vous pouvez ajouter la logique pour récupérer l'historique
            // depuis une table d'audit ou de logs
            
            return response()->json([
                'etablissement' => $etablissement,
                'history' => [
                    // Exemple de structure d'historique
                    [
                        'action' => 'created',
                        'user' => 'Admin Name',
                        'timestamp' => $etablissement->created_at,
                        'changes' => []
                    ],
                    [
                        'action' => 'updated',
                        'user' => 'Admin Name',
                        'timestamp' => $etablissement->updated_at,
                        'changes' => []
                    ]
                ]
            ]);

        } catch (\Exception $e) {
            Log::error("Erreur lors de la récupération de l'historique: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la récupération de l\'historique'
            ], 500);
        }
    }
}