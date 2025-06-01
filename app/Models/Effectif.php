<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Effectif extends Model
{
    use HasFactory;

    protected $fillable = [
        'etablissement_id',
        'sommedenb_eff_g',
        'sommedenb_eff_f',
        'tot',
        'sommedenb_ens_h',
        'sommedenb_ens_f',
        'total_ense'
    ];

    protected $casts = [
        'sommedenb_eff_g' => 'integer',
        'sommedenb_eff_f' => 'integer',
        'tot' => 'integer',
        'sommedenb_ens_h' => 'integer',
        'sommedenb_ens_f' => 'integer',
        'total_ense' => 'integer'
    ];

    public function etablissement()
    {
        return $this->belongsTo(Etablissement::class);
    }

    // Accessors calculés
    public function getTotalElevesAttribute()
    {
        return $this->sommedenb_eff_g + $this->sommedenb_eff_f;
    }

    public function getTotalEnseignantsAttribute()
    {
        return $this->sommedenb_ens_h + $this->sommedenb_ens_f;
    }
}

