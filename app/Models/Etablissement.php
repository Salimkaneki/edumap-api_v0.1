<?php


namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class Etablissement extends Model
{
    use HasFactory;

    protected $fillable = [
        'code_etablissement', 'nom_etablissement', 'region', 'prefecture', 
        'canton_village_autonome', 'ville_village_quartier', 'libelle_type_milieu', 
        'libelle_type_statut_etab', 'libelle_type_systeme', 'existe_elect', 
        'existe_latrine', 'existe_latrine_fonct', 'acces_toute_saison', 'eau', 
        'latitude', 'longitude', 'sommedenb_eff_g', 'sommedenb_eff_f', 'tot', 
        'sommedenb_ens_h', 'sommedenb_ens_f', 'total_ense', 
        'sommedenb_salles_classes_dur', 'sommedenb_salles_classes_banco', 
        'sommedenb_salles_classes_autre', 'libelle_type_annee', 'commune_etab'
    ];

    protected $casts = [
        'existe_elect' => 'boolean',
        'existe_latrine' => 'boolean',
        'existe_latrine_fonct' => 'boolean',
        'acces_toute_saison' => 'boolean',
        'eau' => 'boolean',
    ];
}
