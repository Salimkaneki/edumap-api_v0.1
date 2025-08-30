<?php

namespace Database\Seeders;

use App\Models\Milieu;
use App\Models\Statut;
use App\Models\Systeme;
use App\Models\Annee;
use Illuminate\Database\Seeder;

class ReferenceDataSeeder extends Seeder
{
    /**
     * Run the database seeds.
     */
    public function run(): void
    {
        // Seeder pour les milieux
        $milieux = [
            ['libelle_type_milieu' => 'Rural'],
            ['libelle_type_milieu' => 'Urbain'],
        ];

        foreach ($milieux as $milieu) {
            Milieu::firstOrCreate($milieu);
        }

        // Seeder pour les statuts
        $statuts = [
            ['libelle_type_statut_etab' => 'Public'],
            ['libelle_type_statut_etab' => 'Privé laïc'],
            ['libelle_type_statut_etab' => 'Privé confessionnel'],
        ];

        foreach ($statuts as $statut) {
            Statut::firstOrCreate($statut);
        }

        // Seeder pour les systèmes éducatifs
        $systemes = [
            ['libelle_type_systeme' => 'PRESCOLAIRE'],
            ['libelle_type_systeme' => 'PRIMAIRE'],
            ['libelle_type_systeme' => 'SECONDAIRE I'],
            ['libelle_type_systeme' => 'SECONDAIRE II'],
        ];

        foreach ($systemes as $systeme) {
            Systeme::firstOrCreate($systeme);
        }

        // Seeder pour les années scolaires
        $annees = [
            ['libelle_type_annee' => '2023-2024'],
            ['libelle_type_annee' => '2024-2025'],
        ];

        foreach ($annees as $annee) {
            Annee::firstOrCreate($annee);
        }

        $this->command->info('Données de référence créées avec succès !');
    }
}
