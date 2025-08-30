<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Factories\HasFactory;
class Milieu extends Model
{
    use HasFactory;

        protected $table = 'milieux';


        protected $fillable = [
        'libelle_type_milieu',
        'description',
        'active'
    ];

    protected $casts = [
        'active' => 'boolean',
    ];



    public function etablissements()
    {
        return $this->hasMany(Etablissement::class, 'milieu_id');
    }
}