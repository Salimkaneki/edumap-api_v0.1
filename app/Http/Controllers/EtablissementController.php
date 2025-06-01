<?php

// app/Http/Controllers/EtablissementController.php

namespace App\Http\Controllers;

use App\Models\Etablissement;
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

    // Ajouter un établissement (avec authentification)
    public function store(Request $request)
    {
        $validator = Validator::make($request->all(), [
            'code_etablissement' => 'required|string|unique:etablissements',
            'nom_etablissement' => 'required|string',
            'region' => 'required|string',
            'prefecture' => 'required|string',
            'canton_village_autonome' => 'required|string',
            'ville_village_quartier' => 'required|string',
            'libelle_type_milieu' => 'required|string',
            'libelle_type_statut_etab' => 'required|string',
            'libelle_type_systeme' => 'required|string',
            'existe_elect' => 'boolean',
            'existe_latrine' => 'boolean',
            'existe_latrine_fonct' => 'boolean',
            'acces_toute_saison' => 'boolean',
            'eau' => 'boolean',
            'latitude' => 'required|string',
            'longitude' => 'required|string',
            'sommedenb_eff_g' => 'integer',
            'sommedenb_eff_f' => 'integer',
            'tot' => 'integer',
            'sommedenb_ens_h' => 'integer',
            'sommedenb_ens_f' => 'integer',
            'total_ense' => 'integer',
            'sommedenb_salles_classes_dur' => 'integer',
            'sommedenb_salles_classes_banco' => 'integer',
            'sommedenb_salles_classes_autre' => 'integer',
            'libelle_type_annee' => 'nullable|string',
            'commune_etab' => 'nullable|string',
        ]);

        if ($validator->fails()) {
            return response()->json($validator->errors(), 400);
        }

        $etablissement = Etablissement::create($request->all());
        return response()->json($etablissement, 201);
    }

    // Modifier un établissement
    public function update(Request $request, $id)
    {
        $etablissement = Etablissement::findOrFail($id);

        $validator = Validator::make($request->all(), [
            'code_etablissement' => 'sometimes|string|unique:etablissements,code_etablissement,' . $id,
            'nom_etablissement' => 'sometimes|string',
            'region' => 'sometimes|string',
            'prefecture' => 'sometimes|string',
            'canton_village_autonome' => 'sometimes|string',
            'ville_village_quartier' => 'sometimes|string',
            'libelle_type_milieu' => 'sometimes|string',
            'libelle_type_statut_etab' => 'sometimes|string',
            'libelle_type_systeme' => 'sometimes|string',
            'existe_elect' => 'boolean',
            'existe_latrine' => 'boolean',
            'existe_latrine_fonct' => 'boolean',
            'acces_toute_saison' => 'boolean',
            'eau' => 'boolean',
            'latitude' => 'sometimes|string',
            'longitude' => 'sometimes|string',
            'sommedenb_eff_g' => 'integer',
            'sommedenb_eff_f' => 'integer',
            'tot' => 'integer',
            'sommedenb_ens_h' => 'integer',
            'sommedenb_ens_f' => 'integer',
            'total_ense' => 'integer',
            'sommedenb_salles_classes_dur' => 'integer',
            'sommedenb_salles_classes_banco' => 'integer',
            'sommedenb_salles_classes_autre' => 'integer',
            'libelle_type_annee' => 'nullable|string',
            'commune_etab' => 'nullable|string',
        ]);

        if ($validator->fails()) {
            return response()->json($validator->errors(), 400);
        }

        $etablissement->update($request->all());
        return response()->json($etablissement);
    }

    // Supprimer un établissement
    public function destroy($id)
    {
        $etablissement = Etablissement::findOrFail($id);
        $etablissement->delete();
        return response()->json(null, 204);
    }
}
