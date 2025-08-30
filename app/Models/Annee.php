<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Factories\HasFactory;

class Annee extends Model
{
    use HasFactory;

    protected $fillable = ['libelle_type_annee'];

    public function etablissements()
    {
        return $this->hasMany(Etablissement::class);
    }
}
