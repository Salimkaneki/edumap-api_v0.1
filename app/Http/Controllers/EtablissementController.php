<?php

// app/Http/Controllers/EtablissementController.php

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

class EtablissementController extends Controller
{
    // Récupérer tous les établissements avec pagination
    public function index(Request $request)
    {
        $page = $request->query('page', 1);
        $perPage = $request->query('per_page', 10);

        $cacheKey = 'etablissements_page_' . $page . '_per_page_' . $perPage;
        $etablissements = Cache::remember($cacheKey, 60, function () use ($perPage) {
            return Etablissement::paginate($perPage);
        });

        return response()->json($etablissements);
    }

    public function show($id)
    {
        try {
            Log::info("Recherche de l'établissement avec l'ID: " . $id);
            
            // Convertir en entier pour s'assurer que nous avons le bon format
            $id = (int) $id;
            
            $etablissement = Etablissement::find($id);
            
            if (!$etablissement) {
                Log::warning("Établissement non trouvé avec l'ID: " . $id);
                return response()->json(['error' => 'Établissement non trouvé'], 404);
            }
            
            Log::info("Établissement trouvé:", ['id' => $etablissement->id, 'nom' => $etablissement->nom_etablissement]);
            
            return response()->json($etablissement);
        } catch (\Exception $e) {
            Log::error("Erreur lors de la recherche de l'établissement: " . $e->getMessage(), [
                'id' => $id,
                'exception' => $e->getTraceAsString()
            ]);
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de la recherche de l\'établissement',
                'message' => $e->getMessage()
            ], 500);
        }
    }

    // Rechercher des établissements par différents critères
    public function search(Request $request)
    {
        Log::info('Search method called with params:', $request->all());

        $query = Etablissement::query();

        if ($request->has('nom_etablissement')) {
            $query->where('nom_etablissement', 'like', '%' . $request->nom_etablissement . '%');
        }

        if ($request->has('region')) {
            $query->where('region', $request->region);
        }

        if ($request->has('prefecture')) {
            $query->where('prefecture', $request->prefecture);
        }

        if ($request->has('libelle_type_milieu')) {
            $query->where('libelle_type_milieu', $request->libelle_type_milieu);
        }

        if ($request->has('libelle_type_statut_etab')) {
            $query->where('libelle_type_statut_etab', $request->libelle_type_statut_etab);
        }

        if ($request->has('libelle_type_systeme')) {
            $query->where('libelle_type_systeme', $request->libelle_type_systeme);
        }

        if ($request->has('existe_elect')) {
            $query->where('existe_elect', $request->existe_elect);
        }

        if ($request->has('existe_latrine')) {
            $query->where('existe_latrine', $request->existe_latrine);
        }

        if ($request->has('existe_latrine_fonct')) {
            $query->where('existe_latrine_fonct', $request->existe_latrine_fonct);
        }

        if ($request->has('acces_toute_saison')) {
            $query->where('acces_toute_saison', $request->acces_toute_saison);
        }

        if ($request->has('eau')) {
            $query->where('eau', $request->eau);
        }

        $etablissements = $query->paginate($request->per_page ?? 10);
        return response()->json($etablissements);
    }

    // Récupérer les établissements avec leurs coordonnées pour la carte
    public function map()
    {
        $etablissements = Etablissement::select('id', 'nom_etablissement', 'latitude', 'longitude')->get();
        return response()->json($etablissements);
    }

    // Ajouter un établissement (avec authentification admin)
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
            
            // === CORRECTION : Mapper les libellés vers les IDs ===
            
            // 1. Récupérer l'ID du milieu
            $milieu = Milieu::where('libelle_type_milieu', $data['libelle_type_milieu'])->first();
            if (!$milieu) {
                return response()->json([
                    'error' => 'Type de milieu invalide: ' . $data['libelle_type_milieu']
                ], 422);
            }
            $data['milieu_id'] = $milieu->id;
            
            // 2. Récupérer l'ID du statut
            $statut = Statut::where('libelle_type_statut_etab', $data['libelle_type_statut_etab'])->first();
            if (!$statut) {
                return response()->json([
                    'error' => 'Type de statut invalide: ' . $data['libelle_type_statut_etab']
                ], 422);
            }
            $data['statut_id'] = $statut->id;
            
            // 3. Récupérer l'ID du système
            $systeme = Systeme::where('libelle_type_systeme', $data['libelle_type_systeme'])->first();
            if (!$systeme) {
                return response()->json([
                    'error' => 'Type de système invalide: ' . $data['libelle_type_systeme']
                ], 422);
            }
            $data['systeme_id'] = $systeme->id;
            
            // 4. Récupérer l'ID de l'année (optionnel)
            if (!empty($data['libelle_type_annee'])) {
                $annee = Annee::where('libelle_type_annee', $data['libelle_type_annee'])->first();
                if ($annee) {
                    $data['annee_id'] = $annee->id;
                }
            }
            
            // 5. Gérer la localisation (optionnel)
            // Vous pouvez ajouter la logique pour mapper region/prefecture vers localisation_id
            // Pour l'instant, on laisse localisation_id à null (c'est nullable dans la migration)
            
            // === FIN CORRECTION ===

            // Calculer automatiquement certains totaux si pas fournis
            if (!isset($data['tot']) || $data['tot'] === null) {
                $data['tot'] = ($data['sommedenb_eff_g'] ?? 0) + ($data['sommedenb_eff_f'] ?? 0);
            }
            
            if (!isset($data['total_ense']) || $data['total_ense'] === null) {
                $data['total_ense'] = ($data['sommedenb_ens_h'] ?? 0) + ($data['sommedenb_ens_f'] ?? 0);
            }

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

        $etablissement->refresh(); // Recharge les relations
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

    // Modifier un établissement (admin uniquement)
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
                'libelle_type_systeme' => 'required|string|in:SECONDAIRE I,SECONDAIRE II,PRIMAIRE,PRESCOLAIRE',
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
            
            // === CORRECTION : Mapper les libellés vers les IDs pour la mise à jour ===
            
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
            
            // === FIN CORRECTION ===
            
            // Recalculer les totaux si les effectifs sont modifiés
            if (isset($data['sommedenb_eff_g']) || isset($data['sommedenb_eff_f'])) {
                $data['tot'] = ($data['sommedenb_eff_g'] ?? $etablissement->sommedenb_eff_g ?? 0) + 
                              ($data['sommedenb_eff_f'] ?? $etablissement->sommedenb_eff_f ?? 0);
            }
            
            if (isset($data['sommedenb_ens_h']) || isset($data['sommedenb_ens_f'])) {
                $data['total_ense'] = ($data['sommedenb_ens_h'] ?? $etablissement->sommedenb_ens_h ?? 0) + 
                                     ($data['sommedenb_ens_f'] ?? $etablissement->sommedenb_ens_f ?? 0);
            }

            $etablissement->update($data);

            // Vider le cache
            Cache::flush();

            Log::info("Établissement modifié avec succès:", [
                'etablissement_id' => $etablissement->id,
                'modified_by' => $admin->name
            ]);

            return response()->json([
                'message' => 'Établissement modifié avec succès',
                'etablissement' => $etablissement
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

    // Supprimer un établissement (superadmin uniquement)
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

    // Obtenir les statistiques des établissements (admin)
    public function stats(Request $request)
    {
        try {
            $stats = [
                'total_etablissements' => Etablissement::count(),
                'par_region' => Etablissement::select('region')
                    ->selectRaw('COUNT(*) as count')
                    ->groupBy('region')
                    ->get(),
                'par_type_milieu' => Etablissement::select('libelle_type_milieu')
                    ->selectRaw('COUNT(*) as count')
                    ->groupBy('libelle_type_milieu')
                    ->get(),
                'par_statut' => Etablissement::select('libelle_type_statut_etab')
                    ->selectRaw('COUNT(*) as count')
                    ->groupBy('libelle_type_statut_etab')
                    ->get(),
                'total_eleves' => Etablissement::sum('tot'),
                'total_enseignants' => Etablissement::sum('total_ense'),
                'avec_electricite' => Etablissement::where('existe_elect', true)->count(),
                'avec_latrines' => Etablissement::where('existe_latrine', true)->count(),
                'avec_eau' => Etablissement::where('eau', true)->count(),
            ];

            return response()->json($stats);

        } catch (\Exception $e) {
            Log::error("Erreur lors du calcul des statistiques: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors du calcul des statistiques'
            ], 500);
        }
    }
}