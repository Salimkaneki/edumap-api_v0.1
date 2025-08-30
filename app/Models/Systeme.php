<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Factories\HasFactory;

class Systeme extends Model
{
    use HasFactory;

    protected $fillable = ['libelle_type_systeme'];

    public function etablissements()
    {
        return $this->hasMany(Etablissement::class);
    }
}