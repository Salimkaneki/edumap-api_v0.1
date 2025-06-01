<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Localisation extends Model
{
    //
    use HasFactory;

    protected$fillable = [
        'region',
        'prefecture',
        'canton_village_autonome',
        'ville_village_quartier',
        'commune_etab'
    ];


    public function etablissements()
    {
        return $this->hasMany(Etablissement::class);
    }
}
