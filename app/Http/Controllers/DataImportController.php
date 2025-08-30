<?php

namespace App\Http\Controllers;

use App\Models\Etablissement;
use App\Models\Localisation;
use App\Models\Milieu;
use App\Models\Statut;
use App\Models\Systeme;
use App\Models\Annee;
use App\Models\Effectif;
use App\Models\Infrastructure;
use App\Models\EquipementEtablissement;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use Maatwebsite\Excel\Facades\Excel;

class DataImportController extends Controller
{
    /**
     * Import des données Excel vers la base de données
     */
    public function importExcel(Request $request)
    {
        $request->validate([
            'file' => 'required|mimes:xlsx,xls|max:20480' // 20MB max
        ]);

        try {
            DB::beginTransaction();

            $file = $request->file('file');
            $data = Excel::toArray([], $file)[0]; // Premier onglet
            
            // Supprimer la ligne d'en-tête
            $headers = array_shift($data);
            
            Log::info("Import démarré - " . count($data) . " lignes à traiter");

            $imported = 0;
            $errors = [];

            foreach ($data as $index => $row) {
                try {
                    $rowData = array_combine($headers, $row);
                    $this->processRow($rowData, $index + 2); // +2 car on commence à la ligne 2
                    $imported++;
                    
                    // Log de progression tous les 100 enregistrements
                    if ($imported % 100 === 0) {
                        Log::info("Import progression: $imported/$" . count($data));
                    }
                    
                } catch (\Exception $e) {
                    $errors[] = [
                        'line' => $index + 2,
                        'error' => $e->getMessage(),
                        'data' => $rowData ?? null
                    ];
                    
                    Log::error("Erreur ligne " . ($index + 2) . ": " . $e->getMessage());
                    
                    // Arrêter si trop d'erreurs
                    if (count($errors) > 50) {
                        throw new \Exception("Trop d'erreurs détectées, import annulé");
                    }
                }
            }

            DB::commit();

            Log::info("Import terminé: $imported enregistrements importés, " . count($errors) . " erreurs");

            return response()->json([
                'success' => true,
                'message' => "Import réussi: $imported établissements importés",
                'imported_count' => $imported,
                'error_count' => count($errors),
                'errors' => array_slice($errors, 0, 10) // Première 10 erreurs seulement
            ]);

        } catch (\Exception $e) {
            DB::rollBack();
            
            Log::error("Erreur lors de l'import: " . $e->getMessage());
            
            return response()->json([
                'success' => false,
                'message' => 'Erreur lors de l\'import: ' . $e->getMessage(),
                'errors' => $errors ?? []
            ], 500);
        }
    }

    /**
     * Traiter une ligne de données
     */
    private function processRow(array $rowData, int $lineNumber)
    {
        // Nettoyer et mapper les colonnes selon votre fichier Excel
        $cleanData = $this->cleanRowData($rowData);

        // 1. Créer ou récupérer la localisation
        $localisation = $this->getOrCreateLocalisation($cleanData);

        // 2. Créer ou récupérer le milieu
        $milieu = $this->getOrCreateMilieu($cleanData['libelle_type_milieu'] ?? 'Rural');

        // 3. Créer ou récupérer le statut
        $statut = $this->getOrCreateStatut($cleanData['libelle_type_statut_etab'] ?? 'Public');

        // 4. Créer ou récupérer le système
        $systeme = $this->getOrCreateSysteme($cleanData['libelle_type_systeme'] ?? 'PRIMAIRE');

        // 5. Créer ou récupérer l'année
        $annee = $this->getOrCreateAnnee($cleanData['libelle_type_annee'] ?? '2023-2024');

        // 6. Créer l'établissement
        $etablissement = Etablissement::create([
            'code_etablissement' => $cleanData['code_etablissement'],
            'nom_etablissement' => $cleanData['nom_etablissement'],
            'latitude' => $cleanData['latitude'],
            'longitude' => $cleanData['longitude'],
            'localisation_id' => $localisation->id,
            'milieu_id' => $milieu->id,
            'statut_id' => $statut->id,
            'systeme_id' => $systeme->id,
            'annee_id' => $annee->id,
        ]);

        // 7. Créer les effectifs
        if ($this->hasEffectifData($cleanData)) {
            Effectif::create([
                'etablissement_id' => $etablissement->id,
                'sommedenb_eff_g' => $cleanData['sommedenb_eff_g'] ?? 0,
                'sommedenb_eff_f' => $cleanData['sommedenb_eff_f'] ?? 0,
                'tot' => $cleanData['tot'] ?? 0,
                'sommedenb_ens_h' => $cleanData['sommedenb_ens_h'] ?? 0,
                'sommedenb_ens_f' => $cleanData['sommedenb_ens_f'] ?? 0,
                'total_ense' => $cleanData['total_ense'] ?? 0,
            ]);
        }

        // 8. Créer les infrastructures
        if ($this->hasInfrastructureData($cleanData)) {
            Infrastructure::create([
                'etablissement_id' => $etablissement->id,
                'sommedenb_salles_classes_dur' => $cleanData['sommedenb_salles_classes_dur'] ?? 0,
                'sommedenb_salles_classes_banco' => $cleanData['sommedenb_salles_classes_banco'] ?? 0,
                'sommedenb_salles_classes_autre' => $cleanData['sommedenb_salles_classes_autre'] ?? 0,
            ]);
        }

        // 9. Créer les équipements
        if ($this->hasEquipementData($cleanData)) {
            EquipementEtablissement::create([
                'etablissement_id' => $etablissement->id,
                'existe_elect' => $cleanData['existe_elect'] ?? false,
                'existe_latrine' => $cleanData['existe_latrine'] ?? false,
                'existe_latrine_fonct' => $cleanData['existe_latrine_fonct'] ?? false,
                'acces_toute_saison' => $cleanData['acces_toute_saison'] ?? false,
                'eau' => $cleanData['eau'] ?? false,
            ]);
        }

        Log::debug("Établissement créé: " . $etablissement->nom_etablissement);
    }

    /**
     * Nettoyer les données d'une ligne
     */
    private function cleanRowData(array $rowData): array
    {
        $cleaned = [];

        // Mapper les colonnes de votre Excel (ajustez selon vos noms de colonnes)
        $mapping = [
            'code_etablissement' => 'code_etablissement',
            'nom_etablissement' => 'nom_etablissement',
            'LATITUDE' => 'latitude',
            'LONGITUDE' => 'longitude',
            'région' => 'region',
            'préfecture' => 'prefecture',
            'canton/village' => 'canton_village_autonome',
            'ville/quartier' => 'ville_village_quartier',
            'commune' => 'commune_etab',
            'statut' => 'libelle_type_statut_etab',
            // Ajoutez les autres mappings selon vos colonnes Excel
        ];

        foreach ($mapping as $excelColumn => $dbColumn) {
            $value = $rowData[$excelColumn] ?? null;
            $cleaned[$dbColumn] = $this->cleanValue($value);
        }

        return $cleaned;
    }

    private function cleanValue($value)
    {
        if (is_null($value) || $value === '') {
            return null;
        }

        // Nettoyer les chaînes
        if (is_string($value)) {
            $value = trim($value);
            return $value === '' ? null : $value;
        }

        // Nettoyer les booléens (0/1)
        if (in_array($value, [0, 1, '0', '1'])) {
            return (bool) $value;
        }

        return $value;
    }

    private function getOrCreateLocalisation(array $data): Localisation
    {
        return Localisation::firstOrCreate([
            'region' => $data['region'],
            'prefecture' => $data['prefecture'],
            'canton_village_autonome' => $data['canton_village_autonome'],
            'ville_village_quartier' => $data['ville_village_quartier'],
        ], [
            'commune_etab' => $data['commune_etab'] ?? null,
        ]);
    }

    private function getOrCreateMilieu(string $libelle): Milieu
    {
        return Milieu::firstOrCreate(['libelle_type_milieu' => $libelle]);
    }

    private function getOrCreateStatut(string $libelle): Statut
    {
        return Statut::firstOrCreate(['libelle_type_statut_etab' => $libelle]);
    }

    private function getOrCreateSysteme(string $libelle): Systeme
    {
        return Systeme::firstOrCreate(['libelle_type_systeme' => $libelle]);
    }

    private function getOrCreateAnnee(string $libelle): Annee
    {
        return Annee::firstOrCreate(['libelle_type_annee' => $libelle]);
    }

    private function hasEffectifData(array $data): bool
    {
        return isset($data['sommedenb_eff_g']) || isset($data['sommedenb_eff_f']) || isset($data['tot']);
    }

    private function hasInfrastructureData(array $data): bool
    {
        return isset($data['sommedenb_salles_classes_dur']) || 
               isset($data['sommedenb_salles_classes_banco']) || 
               isset($data['sommedenb_salles_classes_autre']);
    }

    private function hasEquipementData(array $data): bool
    {
        return isset($data['existe_elect']) || isset($data['existe_latrine']) || isset($data['eau']);
    }
}
