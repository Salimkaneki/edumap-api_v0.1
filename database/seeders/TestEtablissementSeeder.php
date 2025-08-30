<?php

namespace Database\Seeders;

use App\Models\Etablissement;
use App\Models\Localisation;
use App\Models\Milieu;
use App\Models\Statut;
use App\Models\Systeme;
use App\Models\Annee;
use App\Models\Effectif;
use App\Models\Infrastructure;
use App\Models\EquipementEtablissement;
use Illuminate\Database\Seeder;

class TestEtablissementSeeder extends Seeder
{
    /**
     * Run the database seeds.
     */
    public function run(): void
    {
        // Récupérer les données de référence
        $milieu_urbain = Milieu::where('libelle_type_milieu', 'Urbain')->first();
        $milieu_rural = Milieu::where('libelle_type_milieu', 'Rural')->first();
        $statut_public = Statut::where('libelle_type_statut_etab', 'Public')->first();
        $statut_prive = Statut::where('libelle_type_statut_etab', 'Privé laïc')->first();
        $systeme_primaire = Systeme::where('libelle_type_systeme', 'PRIMAIRE')->first();
        $systeme_secondaire = Systeme::where('libelle_type_systeme', 'SECONDAIRE I')->first();
        $annee = Annee::where('libelle_type_annee', '2023-2024')->first();

        // Créer quelques localisations
        $lomé = Localisation::create([
            'region' => 'Grand Lomé',
            'prefecture' => 'Golfe',
            'canton_village_autonome' => 'Lomé',
            'ville_village_quartier' => 'Centre Ville',
            'commune_etab' => 'Lomé'
        ]);

        $maritime = Localisation::create([
            'region' => 'Maritime',
            'prefecture' => 'Vo',
            'canton_village_autonome' => 'Vogan',
            'ville_village_quartier' => 'Vogan Centre',
            'commune_etab' => 'Vogan'
        ]);

        // Créer des établissements de test
        $etablissements = [
            [
                'code_etablissement' => 'CPL_ELOHIM_001',
                'nom_etablissement' => 'CPL ELOHIM',
                'latitude' => 6.1319,
                'longitude' => 1.2228,
                'localisation_id' => $lomé->id,
                'milieu_id' => $milieu_urbain->id,
                'statut_id' => $statut_prive->id,
                'systeme_id' => $systeme_primaire->id,
                'annee_id' => $annee->id,
            ],
            [
                'code_etablissement' => 'EPP_CENTRE_002',
                'nom_etablissement' => 'École Primaire Publique Centre',
                'latitude' => 6.1720,
                'longitude' => 1.2312,
                'localisation_id' => $lomé->id,
                'milieu_id' => $milieu_urbain->id,
                'statut_id' => $statut_public->id,
                'systeme_id' => $systeme_primaire->id,
                'annee_id' => $annee->id,
            ],
            [
                'code_etablissement' => 'CEG_VOGAN_003',
                'nom_etablissement' => 'CEG Vogan',
                'latitude' => 6.3419,
                'longitude' => 1.5228,
                'localisation_id' => $maritime->id,
                'milieu_id' => $milieu_rural->id,
                'statut_id' => $statut_public->id,
                'systeme_id' => $systeme_secondaire->id,
                'annee_id' => $annee->id,
            ]
        ];

        foreach ($etablissements as $etabData) {
            // Créer l'établissement
            $etablissement = Etablissement::create($etabData);

            // Ajouter des effectifs
            Effectif::create([
                'etablissement_id' => $etablissement->id,
                'sommedenb_eff_g' => rand(100, 200),
                'sommedenb_eff_f' => rand(80, 180),
                'tot' => rand(180, 380),
                'sommedenb_ens_h' => rand(3, 8),
                'sommedenb_ens_f' => rand(5, 12),
                'total_ense' => rand(8, 20),
            ]);

            // Ajouter des infrastructures
            Infrastructure::create([
                'etablissement_id' => $etablissement->id,
                'sommedenb_salles_classes_dur' => rand(4, 10),
                'sommedenb_salles_classes_banco' => rand(0, 3),
                'sommedenb_salles_classes_autre' => rand(0, 2),
            ]);

            // Ajouter des équipements
            EquipementEtablissement::create([
                'etablissement_id' => $etablissement->id,
                'existe_elect' => rand(0, 1),
                'existe_latrine' => rand(0, 1),
                'existe_latrine_fonct' => rand(0, 1),
                'acces_toute_saison' => rand(0, 1),
                'eau' => rand(0, 1),
            ]);
        }

        $this->command->info('Établissements de test créés avec succès !');
        $this->command->info('- CPL ELOHIM');
        $this->command->info('- École Primaire Publique Centre');
        $this->command->info('- CEG Vogan');
    }
}
