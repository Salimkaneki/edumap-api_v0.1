<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Milieu extends Model
{
    use HasFactory;

    protected $fillable = ['libelle_type_milieu'];

    public function etablissements()
    {
        return $this->hasMany(Etablissement::class);
    }
}