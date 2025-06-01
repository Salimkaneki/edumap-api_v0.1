<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Infrastructure extends Model
{
    use HasFactory;

    protected $fillable = [
        'etablissement_id',
        'sommedenb_salles_classes_dur',
        'sommedenb_salles_classes_banco',
        'sommedenb_salles_classes_autre'
    ];

    protected $casts = [
        'sommedenb_salles_classes_dur' => 'integer',
        'sommedenb_salles_classes_banco' => 'integer',
        'sommedenb_salles_classes_autre' => 'integer'
    ];

    public function etablissement()
    {
        return $this->belongsTo(Etablissement::class);
    }

    // Accessor pour le total des salles
    public function getTotalSallesAttribute()
    {
        return $this->sommedenb_salles_classes_dur + 
               $this->sommedenb_salles_classes_banco + 
               $this->sommedenb_salles_classes_autre;
    }
}
