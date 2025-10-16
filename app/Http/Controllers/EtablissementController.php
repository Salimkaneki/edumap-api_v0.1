<?php

// app/Http/Controllers/EtablissementController.php

namespace App\Http\Controllers;

use App\Models\Etablissement;
use App\Models\Localisation;
use App\Models\Milieu;
use App\Models\Statut;
use App\Models\Systeme;
use App\Models\Annee;
use App\Models\Effectif;
use App\Models\EquipementEtablissement;
use App\Models\Infrastructure;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Validator;
use Maatwebsite\Excel\Facades\Excel;
use Illuminate\Support\Facades\DB;
use Barryvdh\DomPDF\Facade\Pdf;

class EtablissementController extends Controller
{
    // Récupérer tous les établissements avec pagination
    public function index(Request $request)
    {
        $page = $request->query('page', 1);
        $perPage = $request->query('per_page', 10);

        $cacheKey = 'etablissements_page_' . $page . '_per_page_' . $perPage;
        $etablissements = Cache::remember($cacheKey, 60, function () use ($perPage) {
            return Etablissement::with(['localisation', 'milieu', 'statut', 'systeme', 'annee', 'effectif', 'equipement', 'infrastructure'])->paginate($perPage);
        });

        // Transformer les données pour chaque établissement
        $etablissements->getCollection()->transform(function ($etablissement) {
            $etablissementData = $etablissement->toArray();
            
            // Transformer les valeurs null en 0 pour les champs numériques
            $numericFields = [
                'sommedenb_eff_g', 'sommedenb_eff_f', 'tot', 'sommedenb_ens_h', 'sommedenb_ens_f', 'total_ense',
                'sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'
            ];
            
            foreach ($numericFields as $field) {
                if (isset($etablissementData[$field]) && $etablissementData[$field] === null) {
                    $etablissementData[$field] = 0;
                }
            }
            
            // Transformer aussi dans les relations
            if (isset($etablissementData['effectif'])) {
                foreach ($numericFields as $field) {
                    if (isset($etablissementData['effectif'][$field]) && $etablissementData['effectif'][$field] === null) {
                        $etablissementData['effectif'][$field] = 0;
                    }
                }
            }
            
            if (isset($etablissementData['infrastructure'])) {
                $infraFields = ['sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'];
                foreach ($infraFields as $field) {
                    if (isset($etablissementData['infrastructure'][$field]) && $etablissementData['infrastructure'][$field] === null) {
                        $etablissementData['infrastructure'][$field] = 0;
                    }
                }
            }
            
            return $etablissementData;
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
            
            // Charger les relations et transformer les données
            $etablissementData = $etablissement->load(['localisation', 'milieu', 'statut', 'systeme', 'annee', 'effectif', 'equipement', 'infrastructure'])->toArray();
            
            // Transformer les valeurs null en 0 pour les champs numériques
            $numericFields = [
                'sommedenb_eff_g', 'sommedenb_eff_f', 'tot', 'sommedenb_ens_h', 'sommedenb_ens_f', 'total_ense',
                'sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'
            ];
            
            foreach ($numericFields as $field) {
                if (isset($etablissementData[$field]) && $etablissementData[$field] === null) {
                    $etablissementData[$field] = 0;
                }
            }
            
            // Transformer aussi dans les relations
            if (isset($etablissementData['effectif'])) {
                foreach ($numericFields as $field) {
                    if (isset($etablissementData['effectif'][$field]) && $etablissementData['effectif'][$field] === null) {
                        $etablissementData['effectif'][$field] = 0;
                    }
                }
            }
            
            if (isset($etablissementData['infrastructure'])) {
                $infraFields = ['sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'];
                foreach ($infraFields as $field) {
                    if (isset($etablissementData['infrastructure'][$field]) && $etablissementData['infrastructure'][$field] === null) {
                        $etablissementData['infrastructure'][$field] = 0;
                    }
                }
            }
            
            return response()->json($etablissementData);
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

        $etablissements = $query->with(['localisation', 'milieu', 'statut', 'systeme', 'annee', 'effectif', 'equipement', 'infrastructure'])
                                ->paginate($request->per_page ?? 10);
        
        // Transformer les données pour chaque établissement
        $etablissements->getCollection()->transform(function ($etablissement) {
            $etablissementData = $etablissement->toArray();
            
            // Transformer les valeurs null en 0 pour les champs numériques
            $numericFields = [
                'sommedenb_eff_g', 'sommedenb_eff_f', 'tot', 'sommedenb_ens_h', 'sommedenb_ens_f', 'total_ense',
                'sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'
            ];
            
            foreach ($numericFields as $field) {
                if (isset($etablissementData[$field]) && $etablissementData[$field] === null) {
                    $etablissementData[$field] = 0;
                }
            }
            
            // Transformer aussi dans les relations
            if (isset($etablissementData['effectif'])) {
                foreach ($numericFields as $field) {
                    if (isset($etablissementData['effectif'][$field]) && $etablissementData['effectif'][$field] === null) {
                        $etablissementData['effectif'][$field] = 0;
                    }
                }
            }
            
            if (isset($etablissementData['infrastructure'])) {
                $infraFields = ['sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'];
                foreach ($infraFields as $field) {
                    if (isset($etablissementData['infrastructure'][$field]) && $etablissementData['infrastructure'][$field] === null) {
                        $etablissementData['infrastructure'][$field] = 0;
                    }
                }
            }
            
            return $etablissementData;
        });
        
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
                'libelle_type_milieu' => 'required|string|in:Rural,Semi Urbain,Urbain',
                'libelle_type_statut_etab' => 'required|string|in:Communautaire,Privé Catholique,Privé Islamique,Privé Laïc,Privé Protestant,Public',
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
            if (isset($data['region']) || isset($data['prefecture']) || isset($data['canton_village_autonome']) || 
                isset($data['ville_village_quartier']) || isset($data['commune_etab'])) {
                
                $localisationData = [
                    'region' => $data['region'] ?? null,
                    'prefecture' => $data['prefecture'] ?? null,
                    'canton_village_autonome' => $data['canton_village_autonome'] ?? null,
                    'ville_village_quartier' => $data['ville_village_quartier'] ?? null,
                    'commune_etab' => $data['commune_etab'] ?? null,
                ];
                
                // Chercher une localisation existante avec ces critères
                $localisation = Localisation::where('region', $localisationData['region'])
                    ->where('prefecture', $localisationData['prefecture'])
                    ->where('canton_village_autonome', $localisationData['canton_village_autonome'])
                    ->where('ville_village_quartier', $localisationData['ville_village_quartier'])
                    ->where('commune_etab', $localisationData['commune_etab'])
                    ->first();
                
                if (!$localisation) {
                    // Créer la localisation si elle n'existe pas
                    $localisation = Localisation::create($localisationData);
                    Log::info("Localisation créée:", $localisationData);
                }
                
                $data['localisation_id'] = $localisation->id;
            }
            
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
            unset($data['region']);
            unset($data['prefecture']);
            unset($data['canton_village_autonome']);
            unset($data['ville_village_quartier']);
            unset($data['commune_etab']);

            $etablissement = Etablissement::create($data);

            // === CRÉATION AUTOMATIQUE DES ENREGISTREMENTS LIÉS ===
            
            // 1. Créer les effectifs si les données sont fournies
            $effectifFields = ['sommedenb_eff_g', 'sommedenb_eff_f', 'tot', 'sommedenb_ens_h', 'sommedenb_ens_f', 'total_ense'];
            $hasEffectifData = false;
            foreach ($effectifFields as $field) {
                if (isset($request[$field]) && $request[$field] !== null) {
                    $hasEffectifData = true;
                    break;
                }
            }
            
            if ($hasEffectifData) {
                Effectif::create([
                    'etablissement_id' => $etablissement->id,
                    'sommedenb_eff_g' => $request['sommedenb_eff_g'] ?? 0,
                    'sommedenb_eff_f' => $request['sommedenb_eff_f'] ?? 0,
                    'tot' => $request['tot'] ?? (($request['sommedenb_eff_g'] ?? 0) + ($request['sommedenb_eff_f'] ?? 0)),
                    'sommedenb_ens_h' => $request['sommedenb_ens_h'] ?? 0,
                    'sommedenb_ens_f' => $request['sommedenb_ens_f'] ?? 0,
                    'total_ense' => $request['total_ense'] ?? (($request['sommedenb_ens_h'] ?? 0) + ($request['sommedenb_ens_f'] ?? 0)),
                ]);
                Log::info("Effectifs créés pour l'établissement:", ['etablissement_id' => $etablissement->id]);
            }
            
            // 2. Créer les équipements si les données sont fournies
            $equipementFields = ['existe_elect', 'existe_latrine', 'existe_latrine_fonct', 'acces_toute_saison', 'eau'];
            $hasEquipementData = false;
            foreach ($equipementFields as $field) {
                if (isset($request[$field]) && $request[$field] !== null) {
                    $hasEquipementData = true;
                    break;
                }
            }
            
            if ($hasEquipementData) {
                EquipementEtablissement::create([
                    'etablissement_id' => $etablissement->id,
                    'existe_elect' => $request['existe_elect'] ?? false,
                    'existe_latrine' => $request['existe_latrine'] ?? false,
                    'existe_latrine_fonct' => $request['existe_latrine_fonct'] ?? false,
                    'acces_toute_saison' => $request['acces_toute_saison'] ?? false,
                    'eau' => $request['eau'] ?? false,
                ]);
                Log::info("Équipements créés pour l'établissement:", ['etablissement_id' => $etablissement->id]);
            }
            
            // 3. Créer les infrastructures si les données sont fournies
            $infrastructureFields = ['sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'];
            $hasInfrastructureData = false;
            foreach ($infrastructureFields as $field) {
                if (isset($request[$field]) && $request[$field] !== null) {
                    $hasInfrastructureData = true;
                    break;
                }
            }
            
            if ($hasInfrastructureData) {
                Infrastructure::create([
                    'etablissement_id' => $etablissement->id,
                    'sommedenb_salles_classes_dur' => $request['sommedenb_salles_classes_dur'] ?? 0,
                    'sommedenb_salles_classes_banco' => $request['sommedenb_salles_classes_banco'] ?? 0,
                    'sommedenb_salles_classes_autre' => $request['sommedenb_salles_classes_autre'] ?? 0,
                ]);
                Log::info("Infrastructures créées pour l'établissement:", ['etablissement_id' => $etablissement->id]);
            }
            
            // === FIN CRÉATION AUTOMATIQUE ===

            // Vider le cache
            Cache::flush();

            Log::info("Établissement créé avec succès:", [
                'etablissement_id' => $etablissement->id,
                'nom' => $etablissement->nom_etablissement,
                'created_by' => $admin->name
            ]);

        $etablissement->refresh(); // Recharge les relations
        
        // Transformer les valeurs null en 0 pour les champs numériques
        $etablissementData = $etablissement->load(['milieu', 'statut', 'systeme', 'annee', 'effectif', 'equipement', 'infrastructure'])->toArray();
        
        // Champs numériques à transformer
        $numericFields = [
            'sommedenb_eff_g', 'sommedenb_eff_f', 'tot', 'sommedenb_ens_h', 'sommedenb_ens_f', 'total_ense',
            'sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'
        ];
        
        foreach ($numericFields as $field) {
            if (isset($etablissementData[$field]) && $etablissementData[$field] === null) {
                $etablissementData[$field] = 0;
            }
        }
        
        // Transformer aussi dans les relations
        if (isset($etablissementData['effectif'])) {
            foreach ($numericFields as $field) {
                if (isset($etablissementData['effectif'][$field]) && $etablissementData['effectif'][$field] === null) {
                    $etablissementData['effectif'][$field] = 0;
                }
            }
        }
        
        if (isset($etablissementData['infrastructure'])) {
            $infraFields = ['sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'];
            foreach ($infraFields as $field) {
                if (isset($etablissementData['infrastructure'][$field]) && $etablissementData['infrastructure'][$field] === null) {
                    $etablissementData['infrastructure'][$field] = 0;
                }
            }
        }
        
        return response()->json([
            'message' => 'Établissement créé avec succès',
            'data' => $etablissementData
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
                'libelle_type_milieu' => 'sometimes|string|in:Rural,Semi Urbain,Urbain',
                'libelle_type_statut_etab' => 'sometimes|string|in:Communautaire,Privé Catholique,Privé Islamique,Privé Laïc,Privé Protestant,Public',
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
            
            // Gérer la localisation (optionnel)
            if (isset($data['region']) || isset($data['prefecture']) || isset($data['canton_village_autonome']) || 
                isset($data['ville_village_quartier']) || isset($data['commune_etab'])) {
                
                $localisationData = [
                    'region' => $data['region'] ?? $etablissement->localisation->region ?? null,
                    'prefecture' => $data['prefecture'] ?? $etablissement->localisation->prefecture ?? null,
                    'canton_village_autonome' => $data['canton_village_autonome'] ?? $etablissement->localisation->canton_village_autonome ?? null,
                    'ville_village_quartier' => $data['ville_village_quartier'] ?? $etablissement->localisation->ville_village_quartier ?? null,
                    'commune_etab' => $data['commune_etab'] ?? $etablissement->localisation->commune_etab ?? null,
                ];
                
                // Chercher une localisation existante avec ces critères
                $localisation = Localisation::where('region', $localisationData['region'])
                    ->where('prefecture', $localisationData['prefecture'])
                    ->where('canton_village_autonome', $localisationData['canton_village_autonome'])
                    ->where('ville_village_quartier', $localisationData['ville_village_quartier'])
                    ->where('commune_etab', $localisationData['commune_etab'])
                    ->first();
                
                if (!$localisation) {
                    // Créer la localisation si elle n'existe pas
                    $localisation = Localisation::create($localisationData);
                    Log::info("Localisation créée:", $localisationData);
                }
                
                $data['localisation_id'] = $localisation->id;
                
                // Supprimer les champs de localisation du data pour éviter les erreurs d'insertion
                unset($data['region'], $data['prefecture'], $data['canton_village_autonome'], $data['ville_village_quartier'], $data['commune_etab']);
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

            // === AJOUT : Création automatique des enregistrements liés si les données sont fournies ===
            
            // Créer l'effectif si les données sont fournies
            if (isset($data['sommedenb_eff_g']) || isset($data['sommedenb_eff_f']) || isset($data['tot']) ||
                isset($data['sommedenb_ens_h']) || isset($data['sommedenb_ens_f']) || isset($data['total_ense'])) {
                
                Effectif::updateOrCreate(
                    ['etablissement_id' => $etablissement->id],
                    [
                        'sommedenb_eff_g' => $data['sommedenb_eff_g'] ?? $etablissement->sommedenb_eff_g ?? 0,
                        'sommedenb_eff_f' => $data['sommedenb_eff_f'] ?? $etablissement->sommedenb_eff_f ?? 0,
                        'tot' => $data['tot'] ?? $etablissement->tot ?? 0,
                        'sommedenb_ens_h' => $data['sommedenb_ens_h'] ?? $etablissement->sommedenb_ens_h ?? 0,
                        'sommedenb_ens_f' => $data['sommedenb_ens_f'] ?? $etablissement->sommedenb_ens_f ?? 0,
                        'total_ense' => $data['total_ense'] ?? $etablissement->total_ense ?? 0,
                    ]
                );
            }

            // Créer l'équipement si les données sont fournies
            if (isset($data['existe_elect']) || isset($data['existe_latrine']) || isset($data['existe_latrine_fonct']) ||
                isset($data['acces_toute_saison']) || isset($data['eau'])) {
                
                EquipementEtablissement::updateOrCreate(
                    ['etablissement_id' => $etablissement->id],
                    [
                        'existe_elect' => $data['existe_elect'] ?? $etablissement->existe_elect ?? false,
                        'existe_latrine' => $data['existe_latrine'] ?? $etablissement->existe_latrine ?? false,
                        'existe_latrine_fonct' => $data['existe_latrine_fonct'] ?? $etablissement->existe_latrine_fonct ?? false,
                        'acces_toute_saison' => $data['acces_toute_saison'] ?? $etablissement->acces_toute_saison ?? false,
                        'eau' => $data['eau'] ?? $etablissement->eau ?? false,
                    ]
                );
            }

            // Créer l'infrastructure si les données sont fournies
            if (isset($data['sommedenb_salles_classes_dur']) || isset($data['sommedenb_salles_classes_banco']) || 
                isset($data['sommedenb_salles_classes_autre'])) {
                
                Infrastructure::updateOrCreate(
                    ['etablissement_id' => $etablissement->id],
                    [
                        'sommedenb_salles_classes_dur' => $data['sommedenb_salles_classes_dur'] ?? $etablissement->sommedenb_salles_classes_dur ?? 0,
                        'sommedenb_salles_classes_banco' => $data['sommedenb_salles_classes_banco'] ?? $etablissement->sommedenb_salles_classes_banco ?? 0,
                        'sommedenb_salles_classes_autre' => $data['sommedenb_salles_classes_autre'] ?? $etablissement->sommedenb_salles_classes_autre ?? 0,
                    ]
                );
            }

            // === FIN AJOUT ===

            // Vider le cache
            Cache::flush();

            Log::info("Établissement modifié avec succès:", [
                'etablissement_id' => $etablissement->id,
                'modified_by' => $admin->name
            ]);

            // Charger les relations pour la réponse
            $etablissement->load(['localisation', 'milieu', 'statut', 'systeme', 'annee', 'effectif', 'equipement', 'infrastructure']);
            
            // Transformer les valeurs null en 0 pour les champs numériques
            $etablissementData = $etablissement->toArray();
            
            // Champs numériques à transformer
            $numericFields = [
                'sommedenb_eff_g', 'sommedenb_eff_f', 'tot', 'sommedenb_ens_h', 'sommedenb_ens_f', 'total_ense',
                'sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'
            ];
            
            foreach ($numericFields as $field) {
                if (isset($etablissementData[$field]) && $etablissementData[$field] === null) {
                    $etablissementData[$field] = 0;
                }
            }
            
            // Transformer aussi dans les relations
            if (isset($etablissementData['effectif'])) {
                foreach ($numericFields as $field) {
                    if (isset($etablissementData['effectif'][$field]) && $etablissementData['effectif'][$field] === null) {
                        $etablissementData['effectif'][$field] = 0;
                    }
                }
            }
            
            if (isset($etablissementData['infrastructure'])) {
                $infraFields = ['sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'];
                foreach ($infraFields as $field) {
                    if (isset($etablissementData['infrastructure'][$field]) && $etablissementData['infrastructure'][$field] === null) {
                        $etablissementData['infrastructure'][$field] = 0;
                    }
                }
            }

            return response()->json([
                'message' => 'Établissement modifié avec succès',
                'etablissement' => $etablissementData
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
            // Compter les établissements avec des relations manquantes
            $missingRelations = [
                'sans_localisation' => Etablissement::whereNull('localisation_id')->count(),
                'sans_milieu' => Etablissement::whereNull('milieu_id')->count(),
                'sans_statut' => Etablissement::whereNull('statut_id')->count(),
                'sans_systeme' => Etablissement::whereNull('systeme_id')->count(),
            ];

            $stats = [
                'total_etablissements' => Etablissement::count(),
                'relations_manquantes' => $missingRelations,
                
                // Statistiques par région (avec gestion des nulls)
                'par_region' => Etablissement::leftJoin('localisations', 'etablissements.localisation_id', '=', 'localisations.id')
                    ->selectRaw('IFNULL(localisations.region, "Non spécifiée") as region')
                    ->selectRaw('COUNT(*) as count')
                    ->groupByRaw('IFNULL(localisations.region, "Non spécifiée")')
                    ->orderBy('count', 'desc')
                    ->get(),
                
                // Statistiques par préfecture (avec gestion des nulls)  
                'par_prefecture' => Etablissement::leftJoin('localisations', 'etablissements.localisation_id', '=', 'localisations.id')
                    ->selectRaw('IFNULL(localisations.prefecture, "Non spécifiée") as prefecture')
                    ->selectRaw('COUNT(*) as count')
                    ->groupByRaw('IFNULL(localisations.prefecture, "Non spécifiée")')
                    ->orderBy('count', 'desc')
                    ->limit(10)
                    ->get(),
                
                // Statistiques par type de milieu
                'par_type_milieu' => Etablissement::leftJoin('milieux', 'etablissements.milieu_id', '=', 'milieux.id')
                    ->selectRaw('IFNULL(milieux.libelle_type_milieu, "Non spécifié") as libelle_type_milieu')
                    ->selectRaw('COUNT(*) as count')
                    ->groupByRaw('IFNULL(milieux.libelle_type_milieu, "Non spécifié")')
                    ->get(),
                
                // Statistiques par statut
                'par_statut' => Etablissement::leftJoin('statuts', 'etablissements.statut_id', '=', 'statuts.id')
                    ->selectRaw('IFNULL(statuts.libelle_type_statut_etab, "Non spécifié") as libelle_type_statut_etab')
                    ->selectRaw('COUNT(*) as count')
                    ->groupByRaw('IFNULL(statuts.libelle_type_statut_etab, "Non spécifié")')
                    ->get(),
                
                // Statistiques par système
                'par_systeme' => Etablissement::leftJoin('systemes', 'etablissements.systeme_id', '=', 'systemes.id')
                    ->selectRaw('IFNULL(systemes.libelle_type_systeme, "Non spécifié") as libelle_type_systeme')
                    ->selectRaw('COUNT(*) as count')
                    ->groupByRaw('IFNULL(systemes.libelle_type_systeme, "Non spécifié")')
                    ->get(),
                
                // Effectifs totaux (avec vérification de l'existence des données)
                'effectifs' => [
                    'total_eleves' => Etablissement::join('effectifs', 'etablissements.id', '=', 'effectifs.etablissement_id')
                        ->sum('effectifs.tot') ?: 0,
                    'total_enseignants' => Etablissement::join('effectifs', 'etablissements.id', '=', 'effectifs.etablissement_id')
                        ->sum('effectifs.total_ense') ?: 0,
                    'etablissements_avec_effectifs' => Etablissement::whereExists(function($query) {
                        $query->select('id')->from('effectifs')->whereColumn('effectifs.etablissement_id', 'etablissements.id');
                    })->count(),
                ],
                
                // Équipements (avec vérification de l'existence des données)
                'equipements' => [
                    'avec_electricite' => Etablissement::join('equipements_etablissement', 'etablissements.id', '=', 'equipements_etablissement.etablissement_id')
                        ->where('equipements_etablissement.existe_elect', true)->count(),
                    'avec_latrines' => Etablissement::join('equipements_etablissement', 'etablissements.id', '=', 'equipements_etablissement.etablissement_id')
                        ->where('equipements_etablissement.existe_latrine', true)->count(),
                    'avec_eau' => Etablissement::join('equipements_etablissement', 'etablissements.id', '=', 'equipements_etablissement.etablissement_id')
                        ->where('equipements_etablissement.eau', true)->count(),
                    'avec_acces_toute_saison' => Etablissement::join('equipements_etablissement', 'etablissements.id', '=', 'equipements_etablissement.etablissement_id')
                        ->where('equipements_etablissement.acces_toute_saison', true)->count(),
                    'avec_latrines_fonctionnelles' => Etablissement::join('equipements_etablissement', 'etablissements.id', '=', 'equipements_etablissement.etablissement_id')
                        ->where('equipements_etablissement.existe_latrine_fonct', true)->count(),
                    'etablissements_avec_equipements' => Etablissement::whereExists(function($query) {
                        $query->select('id')->from('equipements_etablissement')->whereColumn('equipements_etablissement.etablissement_id', 'etablissements.id');
                    })->count(),
                ],
                
                // Infrastructures
                'infrastructures' => [
                    'total_salles_classes' => Etablissement::join('infrastructures', 'etablissements.id', '=', 'infrastructures.etablissement_id')
                        ->selectRaw('SUM(infrastructures.sommedenb_salles_classes_dur + infrastructures.sommedenb_salles_classes_banco + infrastructures.sommedenb_salles_classes_autre) as total')
                        ->value('total') ?: 0,
                    'salles_dur' => Etablissement::join('infrastructures', 'etablissements.id', '=', 'infrastructures.etablissement_id')
                        ->sum('infrastructures.sommedenb_salles_classes_dur') ?: 0,
                    'salles_banco' => Etablissement::join('infrastructures', 'etablissements.id', '=', 'infrastructures.etablissement_id')
                        ->sum('infrastructures.sommedenb_salles_classes_banco') ?: 0,
                    'salles_autre' => Etablissement::join('infrastructures', 'etablissements.id', '=', 'infrastructures.etablissement_id')
                        ->sum('infrastructures.sommedenb_salles_classes_autre') ?: 0,
                    'etablissements_avec_infrastructures' => Etablissement::whereExists(function($query) {
                        $query->select('id')->from('infrastructures')->whereColumn('infrastructures.etablissement_id', 'etablissements.id');
                    })->count(),
                ],
                
                // Géolocalisation
                'geolocalisation' => [
                    'avec_coordonnees' => Etablissement::whereNotNull('latitude')->whereNotNull('longitude')->count(),
                    'sans_coordonnees' => Etablissement::where(function($query) {
                        $query->whereNull('latitude')->orWhereNull('longitude');
                    })->count(),
                ],
            ];

            return response()->json($stats);

        } catch (\Exception $e) {
            Log::error("Erreur lors du calcul des statistiques: " . $e->getMessage());
            
            return response()->json([
                'error' => 'Une erreur est survenue lors du calcul des statistiques',
                'message' => $e->getMessage()
            ], 500);
        }
    }

    // Importer des établissements depuis un fichier Excel
    public function import(Request $request)
    {
        try {
            $admin = $request->user();
            Log::info("Admin import établissements:", [
                'admin_id' => $admin->id,
                'admin_name' => $admin->name
            ]);

            // Validation du fichier
            $validator = Validator::make($request->all(), [
                'file' => 'required|file|mimes:xlsx,xls,csv|max:10240', // Max 10MB
            ]);

            if ($validator->fails()) {
                return response()->json([
                    'error' => 'Fichier invalide',
                    'details' => $validator->errors()
                ], 422);
            }

            $file = $request->file('file');
            
            // Créer le dossier imports s'il n'existe pas
            $importDir = storage_path('app/imports');
            if (!file_exists($importDir)) {
                mkdir($importDir, 0755, true);
                chmod($importDir, 0755);
            }
            
            $fileName = time() . '_' . $file->getClientOriginalName();
            
            // Sauvegarder directement avec move au lieu de storeAs
            $fullPath = $importDir . '/' . $fileName;
            $file->move($importDir, $fileName);

            Log::info("Fichier uploadé:", [
                'filename' => $fileName,
                'full_path' => $fullPath,
                'file_exists' => file_exists($fullPath),
                'file_size' => file_exists($fullPath) ? filesize($fullPath) : 0,
                'is_readable' => file_exists($fullPath) ? is_readable($fullPath) : false
            ]);

            // Vérifier que le fichier existe et est lisible
            if (!file_exists($fullPath)) {
                return response()->json([
                    'error' => 'Le fichier n\'a pas pu être sauvegardé',
                    'path' => $fullPath,
                    'directory_exists' => file_exists($importDir),
                    'directory_writable' => is_writable($importDir)
                ], 500);
            }

            if (!is_readable($fullPath)) {
                return response()->json([
                    'error' => 'Le fichier n\'est pas lisible',
                    'path' => $fullPath,
                    'permissions' => substr(sprintf('%o', fileperms($fullPath)), -4)
                ], 500);
            }

            // Lire le fichier Excel avec le chemin complet
            $data = Excel::toArray([], $fullPath);
            
            if (empty($data) || empty($data[0])) {
                return response()->json([
                    'error' => 'Le fichier Excel est vide ou invalide'
                ], 422);
            }

            $rows = $data[0]; // Première feuille
            $headers = array_shift($rows); // Première ligne = en-têtes
            
            Log::info("Headers trouvés:", $headers);

            // Mapper les colonnes (vous devrez ajuster selon votre fichier Excel)
            $columnMapping = [
                'code_etablissement' => 'Code_etab',
                'nom_etablissement' => 'Nom_etab',
                'region' => 'Region',
                'prefecture' => 'Prefecture',
                'canton_village_autonome' => 'Canton_ou_village_autonome',
                'ville_village_quartier' => 'Ville_village_quartier',
                'libelle_type_milieu' => 'Libelle_type_milieu',
                'libelle_type_statut_etab' => 'Libelle_type_statut_etab',
                'libelle_type_systeme' => 'Libelle_type_systeme',
                'existe_elect' => 'Existe_elect',
                'existe_latrine' => 'Existe_latrine',
                'existe_latrine_fonct' => 'Existe_latrine_fonct',
                'acces_toute_saison' => 'Acces_toute_saison',
                'eau' => 'Eau',
                'latitude' => 'Latitude',
                'longitude' => 'Longitude',
                'sommedenb_eff_g' => 'SommedenbEffG',
                'sommedenb_eff_f' => 'SommedenbEffF',
                'tot' => 'Tot',
                'sommedenb_ens_h' => 'SommedenbEnsH',
                'sommedenb_ens_f' => 'SommedenbEnsF',
                'total_ense' => 'TotalEnse',
                'sommedenb_salles_classes_dur' => 'SommedenbSallesClassesDur',
                'sommedenb_salles_classes_banco' => 'SommedenbSallesClassesBanco',
                'sommedenb_salles_classes_autre' => 'SommedenbSallesClassesAutre',
                'libelle_type_annee' => 'Libelle_type_annee',
                'commune_etab' => 'Commune_etab'
            ];

            // Créer un index des colonnes
            $columnIndexes = [];
            foreach ($headers as $index => $header) {
                $columnIndexes[trim($header)] = $index;
            }

            $imported = 0;
            $errors = [];
            $skipped = 0;

            DB::beginTransaction();

            foreach ($rows as $rowIndex => $row) {
                try {
                    $etablissementData = [];
                    
                    // Mapper les données selon le mapping des colonnes
                    foreach ($columnMapping as $dbField => $excelColumn) {
                        if (isset($columnIndexes[$excelColumn])) {
                            $value = $row[$columnIndexes[$excelColumn]] ?? null;
                            
                            // Nettoyer et convertir les valeurs
                            if (in_array($dbField, ['existe_elect', 'existe_latrine', 'existe_latrine_fonct', 'acces_toute_saison', 'eau'])) {
                                // Convertir en booléen
                                $etablissementData[$dbField] = in_array(strtolower(trim($value)), ['oui', 'yes', '1', 'true', 'vrai']);
                            } elseif (in_array($dbField, ['latitude', 'longitude'])) {
                                // Convertir en float
                                $etablissementData[$dbField] = is_numeric($value) ? (float)$value : null;
                            } elseif (in_array($dbField, ['sommedenb_eff_g', 'sommedenb_eff_f', 'tot', 'sommedenb_ens_h', 'sommedenb_ens_f', 'total_ense', 'sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 'sommedenb_salles_classes_autre'])) {
                                // Convertir en entier
                                $etablissementData[$dbField] = is_numeric($value) ? (int)$value : 0;
                            } else {
                                // Garder comme string
                                $etablissementData[$dbField] = trim($value);
                            }
                        }
                    }

                    // Vérifier les champs obligatoires
                    if (empty($etablissementData['code_etablissement']) || empty($etablissementData['nom_etablissement'])) {
                        $errors[] = "Ligne " . ($rowIndex + 2) . ": Code ou nom d'établissement manquant";
                        $skipped++;
                        continue;
                    }

                    // Vérifier si l'établissement existe déjà
                    if (Etablissement::where('code_etablissement', $etablissementData['code_etablissement'])->exists()) {
                        $skipped++;
                        continue;
                    }

                    // Mapper les libellés vers les IDs
                    if (!empty($etablissementData['libelle_type_milieu'])) {
                        $milieu = Milieu::where('libelle_type_milieu', $etablissementData['libelle_type_milieu'])->first();
                        if ($milieu) {
                            $etablissementData['milieu_id'] = $milieu->id;
                        }
                        unset($etablissementData['libelle_type_milieu']);
                    }

                    if (!empty($etablissementData['libelle_type_statut_etab'])) {
                        $statut = Statut::where('libelle_type_statut_etab', $etablissementData['libelle_type_statut_etab'])->first();
                        if ($statut) {
                            $etablissementData['statut_id'] = $statut->id;
                        }
                        unset($etablissementData['libelle_type_statut_etab']);
                    }

                    if (!empty($etablissementData['libelle_type_systeme'])) {
                        $systeme = Systeme::where('libelle_type_systeme', $etablissementData['libelle_type_systeme'])->first();
                        if ($systeme) {
                            $etablissementData['systeme_id'] = $systeme->id;
                        }
                        unset($etablissementData['libelle_type_systeme']);
                    }

                    if (!empty($etablissementData['libelle_type_annee'])) {
                        $annee = Annee::where('libelle_type_annee', $etablissementData['libelle_type_annee'])->first();
                        if ($annee) {
                            $etablissementData['annee_id'] = $annee->id;
                        }
                        unset($etablissementData['libelle_type_annee']);
                    }

                    // Créer l'établissement
                    Etablissement::create($etablissementData);
                    $imported++;

                } catch (\Exception $e) {
                    $errors[] = "Ligne " . ($rowIndex + 2) . ": " . $e->getMessage();
                    Log::error("Erreur import ligne " . ($rowIndex + 2), [
                        'error' => $e->getMessage(),
                        'data' => $etablissementData ?? []
                    ]);
                }
            }

            DB::commit();

            // Supprimer le fichier temporaire
            if (file_exists($fullPath)) {
                unlink($fullPath);
            }

            // Vider le cache
            Cache::flush();

            Log::info("Import terminé:", [
                'imported' => $imported,
                'skipped' => $skipped,
                'errors' => count($errors),
                'admin' => $admin->name
            ]);

            return response()->json([
                'message' => 'Import terminé avec succès',
                'statistics' => [
                    'imported' => $imported,
                    'skipped' => $skipped,
                    'errors' => count($errors)
                ],
                'errors' => $errors
            ], 200);

        } catch (\Exception $e) {
            DB::rollBack();
            
            Log::error("Erreur lors de l'import: " . $e->getMessage(), [
                'exception' => $e->getTraceAsString()
            ]);

            return response()->json([
                'error' => 'Une erreur est survenue lors de l\'import',
                'message' => $e->getMessage()
            ], 500);
        }
    }

    // Exporter les établissements
    public function export(Request $request)
    {
        try {
            $admin = $request->user();
            
            // Valider le format d'export
            $validator = Validator::make($request->all(), [
                'format' => 'required|in:excel,csv,pdf',
                'region' => 'nullable|string',
                'prefecture' => 'nullable|string',
                'libelle_type_milieu' => 'nullable|string',
                'libelle_type_statut_etab' => 'nullable|string',
                'libelle_type_systeme' => 'nullable|string',
            ]);

            if ($validator->fails()) {
                return response()->json([
                    'error' => 'Paramètres invalides',
                    'details' => $validator->errors()
                ], 422);
            }

            $format = $request->format;
            $filters = $request->only(['region', 'prefecture', 'libelle_type_milieu', 'libelle_type_statut_etab', 'libelle_type_systeme']);
            
            Log::info("Admin export établissements:", [
                'admin_id' => $admin->id,
                'admin_name' => $admin->name,
                'format' => $format,
                'filters' => $filters
            ]);

            $fileName = 'etablissements_' . date('Y-m-d_His');

            switch ($format) {
                case 'excel':
                    return Excel::download(
                        new \App\Exports\EtablissementsExport($filters),
                        $fileName . '.xlsx',
                        \Maatwebsite\Excel\Excel::XLSX
                    );

                case 'csv':
                    return Excel::download(
                        new \App\Exports\EtablissementsExport($filters),
                        $fileName . '.csv',
                        \Maatwebsite\Excel\Excel::CSV,
                        [
                            'Content-Type' => 'text/csv',
                        ]
                    );

                case 'pdf':
                    return $this->exportPdf($filters, $fileName);

                default:
                    return response()->json([
                        'error' => 'Format non supporté'
                    ], 400);
            }

        } catch (\Exception $e) {
            Log::error("Erreur lors de l'export: " . $e->getMessage(), [
                'exception' => $e->getTraceAsString()
            ]);
            
            return response()->json([
                'error' => 'Une erreur est survenue lors de l\'export',
                'message' => $e->getMessage()
            ], 500);
        }
    }

    // Exporter en PDF
    private function exportPdf($filters, $fileName)
    {
        $query = Etablissement::with(['localisation', 'milieu', 'statut', 'systeme', 'annee', 'effectif', 'equipement', 'infrastructure']);

        // Appliquer les filtres
        if (!empty($filters['region'])) {
            $query->where('region', $filters['region']);
        }

        if (!empty($filters['prefecture'])) {
            $query->where('prefecture', $filters['prefecture']);
        }

        if (!empty($filters['libelle_type_milieu'])) {
            $query->where('libelle_type_milieu', $filters['libelle_type_milieu']);
        }

        if (!empty($filters['libelle_type_statut_etab'])) {
            $query->where('libelle_type_statut_etab', $filters['libelle_type_statut_etab']);
        }

        if (!empty($filters['libelle_type_systeme'])) {
            $query->where('libelle_type_systeme', $filters['libelle_type_systeme']);
        }

        $etablissements = $query->get();

        // Préparer les statistiques
        $stats = [
            'total' => $etablissements->count(),
            'total_eleves' => $etablissements->sum(function($e) {
                return $e->effectif->tot ?? $e->tot ?? 0;
            }),
            'total_enseignants' => $etablissements->sum(function($e) {
                return $e->effectif->total_ense ?? $e->total_ense ?? 0;
            }),
            'avec_electricite' => $etablissements->filter(function($e) {
                return ($e->equipement->existe_elect ?? $e->existe_elect) == true;
            })->count(),
            'avec_eau' => $etablissements->filter(function($e) {
                return ($e->equipement->eau ?? $e->eau) == true;
            })->count(),
        ];

        $pdf = Pdf::loadView('exports.etablissements-pdf', [
            'etablissements' => $etablissements,
            'stats' => $stats,
            'filters' => $filters,
            'date' => now()->format('d/m/Y H:i'),
        ]);

        $pdf->setPaper('a4', 'landscape');

        return $pdf->download($fileName . '.pdf');
    }
}
