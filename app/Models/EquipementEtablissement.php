<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class EquipementEtablissement extends Model
{
    //
    use HasFactory;

    protected $table = 'equipements_etablissement';

    protected $fillable = [
        'etablissement_id',
        'existe_elect',
        'existe_latrine',
        'existe_latrine_fonct',
        'acces_toute_saison',
        'eau'
    ];

    protected $casts = [
        'existe_elect' => 'boolean',
        'existe_latrine' => 'boolean',
        'existe_latrine_fonct' => 'boolean',
        'acces_toute_saison' => 'boolean',
        'eau' => 'boolean'
    ];

    public function etablissement()
    {
        return $this->belongsTo(Etablissement::class);
    }
}

