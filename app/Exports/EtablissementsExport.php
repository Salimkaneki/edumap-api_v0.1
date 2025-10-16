<?php

namespace App\Exports;

use App\Models\Etablissement;
use Maatwebsite\Excel\Concerns\FromQuery;
use Maatwebsite\Excel\Concerns\WithHeadings;
use Maatwebsite\Excel\Concerns\WithMapping;
use Maatwebsite\Excel\Concerns\WithColumnWidths;
use Maatwebsite\Excel\Concerns\WithStyles;
use Maatwebsite\Excel\Concerns\WithChunkReading;
use PhpOffice\PhpSpreadsheet\Worksheet\Worksheet;

class EtablissementsExport implements FromQuery, WithHeadings, WithMapping, WithColumnWidths, WithStyles, WithChunkReading
{
    protected $filters;

    public function __construct($filters = [])
    {
        $this->filters = $filters;
    }

    /**
     * Récupérer la requête pour l'export (optimisé avec query au lieu de collection)
     */
    public function query()
    {
        $query = Etablissement::query()
            ->select([
                'etablissements.*',
                'localisations.region as loc_region',
                'localisations.prefecture as loc_prefecture',
                'localisations.canton_village_autonome as loc_canton',
                'localisations.ville_village_quartier as loc_ville',
                'localisations.commune_etab as loc_commune',
                'milieux.libelle_type_milieu as milieu_libelle',
                'statuts.libelle_type_statut_etab as statut_libelle',
                'systemes.libelle_type_systeme as systeme_libelle',
                'annees.libelle_type_annee as annee_libelle',
                'effectifs.sommedenb_eff_g as eff_g',
                'effectifs.sommedenb_eff_f as eff_f',
                'effectifs.tot as eff_tot',
                'effectifs.sommedenb_ens_h as ens_h',
                'effectifs.sommedenb_ens_f as ens_f',
                'effectifs.total_ense as ens_total',
                'equipements_etablissement.existe_elect as equip_elect',
                'equipements_etablissement.existe_latrine as equip_latrine',
                'equipements_etablissement.existe_latrine_fonct as equip_latrine_fonct',
                'equipements_etablissement.acces_toute_saison as equip_acces',
                'equipements_etablissement.eau as equip_eau',
                'infrastructures.sommedenb_salles_classes_dur as infra_dur',
                'infrastructures.sommedenb_salles_classes_banco as infra_banco',
                'infrastructures.sommedenb_salles_classes_autre as infra_autre'
            ])
            ->leftJoin('localisations', 'etablissements.localisation_id', '=', 'localisations.id')
            ->leftJoin('milieux', 'etablissements.milieu_id', '=', 'milieux.id')
            ->leftJoin('statuts', 'etablissements.statut_id', '=', 'statuts.id')
            ->leftJoin('systemes', 'etablissements.systeme_id', '=', 'systemes.id')
            ->leftJoin('annees', 'etablissements.annee_id', '=', 'annees.id')
            ->leftJoin('effectifs', 'etablissements.id', '=', 'effectifs.etablissement_id')
            ->leftJoin('equipements_etablissement', 'etablissements.id', '=', 'equipements_etablissement.etablissement_id')
            ->leftJoin('infrastructures', 'etablissements.id', '=', 'infrastructures.etablissement_id');

        // Appliquer les filtres si fournis
        if (!empty($this->filters['region'])) {
            $query->where('etablissements.region', $this->filters['region']);
        }

        if (!empty($this->filters['prefecture'])) {
            $query->where('etablissements.prefecture', $this->filters['prefecture']);
        }

        if (!empty($this->filters['libelle_type_milieu'])) {
            $query->where('etablissements.libelle_type_milieu', $this->filters['libelle_type_milieu']);
        }

        if (!empty($this->filters['libelle_type_statut_etab'])) {
            $query->where('etablissements.libelle_type_statut_etab', $this->filters['libelle_type_statut_etab']);
        }

        if (!empty($this->filters['libelle_type_systeme'])) {
            $query->where('etablissements.libelle_type_systeme', $this->filters['libelle_type_systeme']);
        }

        return $query;
    }

    /**
     * Traiter les données par chunks pour éviter les problèmes de mémoire
     */
    public function chunkSize(): int
    {
        return 500; // Traiter 500 lignes à la fois
    }

    /**
     * Définir les en-têtes des colonnes
     */
    public function headings(): array
    {
        return [
            'ID Établissement',
            'Code Établissement',
            'Nom Établissement',
            'Région',
            'Préfecture',
            'Canton/Village Autonome',
            'Ville/Village/Quartier',
            'Commune',
            'Type de Milieu',
            'Statut',
            'Système',
            'Année',
            'Niveau Enseignement',
            'Effectif Garçons',
            'Effectif Filles',
            'Total Élèves',
            'Enseignants Hommes',
            'Enseignants Femmes',
            'Total Enseignants',
            'Électricité',
            'Latrines',
            'Latrines Fonctionnelles',
            'Accès Toute Saison',
            'Eau',
            'Salles en Dur',
            'Salles en Banco',
            'Autres Salles',
        ];
    }

    /**
     * Mapper les données de chaque établissement (optimisé sans relations)
     */
    public function map($etablissement): array
    {
        return [
            $etablissement->id_etab,
            $etablissement->code_etab,
            $etablissement->nom_etab,
            $etablissement->loc_region ?? $etablissement->region ?? 'N/A',
            $etablissement->loc_prefecture ?? $etablissement->prefecture ?? 'N/A',
            $etablissement->loc_canton ?? $etablissement->canton_village_autonome ?? 'N/A',
            $etablissement->loc_ville ?? $etablissement->ville_village_quartier ?? 'N/A',
            $etablissement->loc_commune ?? $etablissement->commune_etab ?? 'N/A',
            $etablissement->milieu_libelle ?? $etablissement->libelle_type_milieu ?? 'N/A',
            $etablissement->statut_libelle ?? $etablissement->libelle_type_statut_etab ?? 'N/A',
            $etablissement->systeme_libelle ?? $etablissement->libelle_type_systeme ?? 'N/A',
            $etablissement->annee_libelle ?? $etablissement->libelle_type_annee ?? 'N/A',
            $etablissement->niveau_enseignement ?? 'N/A',
            $etablissement->eff_g ?? 0,
            $etablissement->eff_f ?? 0,
            $etablissement->eff_tot ?? 0,
            $etablissement->ens_h ?? 0,
            $etablissement->ens_f ?? 0,
            $etablissement->ens_total ?? 0,
            $etablissement->equip_elect ?? 'Non',
            $etablissement->equip_latrine ?? 'Non',
            $etablissement->equip_latrine_fonct ?? 'Non',
            $etablissement->equip_acces ?? 'Non',
            $etablissement->equip_eau ?? 'Non',
            $etablissement->infra_dur ?? 0,
            $etablissement->infra_banco ?? 0,
            $etablissement->infra_autre ?? 0,
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
