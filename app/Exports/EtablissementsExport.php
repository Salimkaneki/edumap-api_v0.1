<?php

namespace App\Exports;

use App\Models\Etablissement;
use Maatwebsite\Excel\Concerns\FromCollection;
use Maatwebsite\Excel\Concerns\WithHeadings;
use Maatwebsite\Excel\Concerns\WithMapping;
use Maatwebsite\Excel\Concerns\WithColumnWidths;
use Maatwebsite\Excel\Concerns\WithStyles;
use PhpOffice\PhpSpreadsheet\Worksheet\Worksheet;

class EtablissementsExport implements FromCollection, WithHeadings, WithMapping, WithColumnWidths, WithStyles
{
    protected $filters;

    public function __construct($filters = [])
    {
        $this->filters = $filters;
    }

    /**
     * Récupérer les données à exporter
     */
    public function collection()
    {
        $query = Etablissement::with(['localisation', 'milieu', 'statut', 'systeme', 'annee', 'effectif', 'equipement', 'infrastructure']);

        // Appliquer les filtres si fournis
        if (!empty($this->filters['region'])) {
            $query->where('region', $this->filters['region']);
        }

        if (!empty($this->filters['prefecture'])) {
            $query->where('prefecture', $this->filters['prefecture']);
        }

        if (!empty($this->filters['libelle_type_milieu'])) {
            $query->where('libelle_type_milieu', $this->filters['libelle_type_milieu']);
        }

        if (!empty($this->filters['libelle_type_statut_etab'])) {
            $query->where('libelle_type_statut_etab', $this->filters['libelle_type_statut_etab']);
        }

        if (!empty($this->filters['libelle_type_systeme'])) {
            $query->where('libelle_type_systeme', $this->filters['libelle_type_systeme']);
        }

        return $query->get();
    }

    /**
     * Définir les en-têtes des colonnes
     */
    public function headings(): array
    {
        return [
            'Code Établissement',
            'Nom Établissement',
            'Région',
            'Préfecture',
            'Canton/Village Autonome',
            'Ville/Village/Quartier',
            'Commune',
            'Latitude',
            'Longitude',
            'Type de Milieu',
            'Statut',
            'Système',
            'Année',
            'Électricité',
            'Latrines',
            'Latrines Fonctionnelles',
            'Accès Toute Saison',
            'Eau',
            'Effectif Garçons',
            'Effectif Filles',
            'Total Élèves',
            'Enseignants Hommes',
            'Enseignants Femmes',
            'Total Enseignants',
            'Salles en Dur',
            'Salles en Banco',
            'Autres Salles',
        ];
    }

    /**
     * Mapper les données de chaque établissement
     */
    public function map($etablissement): array
    {
        return [
            $etablissement->code_etablissement,
            $etablissement->nom_etablissement,
            $etablissement->localisation->region ?? $etablissement->region ?? 'N/A',
            $etablissement->localisation->prefecture ?? $etablissement->prefecture ?? 'N/A',
            $etablissement->localisation->canton_village_autonome ?? $etablissement->canton_village_autonome ?? 'N/A',
            $etablissement->localisation->ville_village_quartier ?? $etablissement->ville_village_quartier ?? 'N/A',
            $etablissement->localisation->commune_etab ?? $etablissement->commune_etab ?? 'N/A',
            $etablissement->latitude ?? 'N/A',
            $etablissement->longitude ?? 'N/A',
            $etablissement->milieu->libelle_type_milieu ?? $etablissement->libelle_type_milieu ?? 'N/A',
            $etablissement->statut->libelle_type_statut_etab ?? $etablissement->libelle_type_statut_etab ?? 'N/A',
            $etablissement->systeme->libelle_type_systeme ?? $etablissement->libelle_type_systeme ?? 'N/A',
            $etablissement->annee->libelle_type_annee ?? $etablissement->libelle_type_annee ?? 'N/A',
            $etablissement->equipement->existe_elect ?? $etablissement->existe_elect ? 'Oui' : 'Non',
            $etablissement->equipement->existe_latrine ?? $etablissement->existe_latrine ? 'Oui' : 'Non',
            $etablissement->equipement->existe_latrine_fonct ?? $etablissement->existe_latrine_fonct ? 'Oui' : 'Non',
            $etablissement->equipement->acces_toute_saison ?? $etablissement->acces_toute_saison ? 'Oui' : 'Non',
            $etablissement->equipement->eau ?? $etablissement->eau ? 'Oui' : 'Non',
            $etablissement->effectif->sommedenb_eff_g ?? $etablissement->sommedenb_eff_g ?? 0,
            $etablissement->effectif->sommedenb_eff_f ?? $etablissement->sommedenb_eff_f ?? 0,
            $etablissement->effectif->tot ?? $etablissement->tot ?? 0,
            $etablissement->effectif->sommedenb_ens_h ?? $etablissement->sommedenb_ens_h ?? 0,
            $etablissement->effectif->sommedenb_ens_f ?? $etablissement->sommedenb_ens_f ?? 0,
            $etablissement->effectif->total_ense ?? $etablissement->total_ense ?? 0,
            $etablissement->infrastructure->sommedenb_salles_classes_dur ?? $etablissement->sommedenb_salles_classes_dur ?? 0,
            $etablissement->infrastructure->sommedenb_salles_classes_banco ?? $etablissement->sommedenb_salles_classes_banco ?? 0,
            $etablissement->infrastructure->sommedenb_salles_classes_autre ?? $etablissement->sommedenb_salles_classes_autre ?? 0,
        ];
    }

    /**
     * Définir la largeur des colonnes
     */
    public function columnWidths(): array
    {
        return [
            'A' => 15, // Code
            'B' => 30, // Nom
            'C' => 12, // Région
            'D' => 15, // Préfecture
            'E' => 20, // Canton
            'F' => 20, // Ville
            'G' => 15, // Commune
            'H' => 12, // Latitude
            'I' => 12, // Longitude
            'J' => 12, // Milieu
            'K' => 15, // Statut
            'L' => 15, // Système
            'M' => 12, // Année
            'N' => 12, // Électricité
            'O' => 12, // Latrines
            'P' => 15, // Latrines Fonct
            'Q' => 15, // Accès
            'R' => 10, // Eau
            'S' => 12, // Eff. G
            'T' => 12, // Eff. F
            'U' => 12, // Total Élèves
            'V' => 12, // Ens. H
            'W' => 12, // Ens. F
            'X' => 12, // Total Ens.
            'Y' => 12, // Salles Dur
            'Z' => 12, // Salles Banco
            'AA' => 12, // Autres Salles
        ];
    }

    /**
     * Appliquer des styles
     */
    public function styles(Worksheet $sheet)
    {
        return [
            // Style pour la ligne d'en-tête
            1 => [
                'font' => [
                    'bold' => true,
                    'size' => 12,
                ],
                'fill' => [
                    'fillType' => \PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID,
                    'startColor' => ['rgb' => '4472C4'],
                ],
                'font' => [
                    'color' => ['rgb' => 'FFFFFF'],
                    'bold' => true,
                ],
            ],
        ];
    }
}
