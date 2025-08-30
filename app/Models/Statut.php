<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Factories\HasFactory;

class Statut extends Model
{
    use HasFactory;

    protected $fillable = ['libelle_type_statut_etab'];

    public function etablissements()
    {
        return $this->hasMany(Etablissement::class);
    }
}