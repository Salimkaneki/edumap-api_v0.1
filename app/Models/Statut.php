<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Statut extends Model
{
    use HasFactory;

    protected $fillable = ['libelle_type_statut_etab'];

    public function etablissements()
    {
        return $this->hasMany(Etablissement::class);
    }
}