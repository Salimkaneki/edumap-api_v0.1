<?php


namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class Etablissement extends Model
{
    use HasFactory;

    protected $fillable = [
        'code_etablissement',
        'nom_etablissement',
        'latitude',
        'longitude',
        'localisation_id',
        'milieu_id',
        'statut_id',
        'systeme_id',
        'annee_id',

    ];

    protected $casts = [
    	'latitude' => 'decimal:8',
	'longitude' => 'decimal:8'
    ];

    // Relations belongsTo
    public function localisation()
    {
        return $this->belongsTo(Localisation::class);
    }

    public function milieu()
    {
        return $this->belongsTo(Milieu::class);
    }

    public function statut()
    {
        return $this->belongsTo(Statut::class);
    }

    public function systeme()
    {
        return $this->belongsTo(Systeme::class);
    }

    public function annee()
    {
        return $this->belongsTo(Annee::class);
    }

    // Relations hasOne
    public function equipement()
    {
        return $this->hasOne(EquipementEtablissement::class);
    }

    public function effectif()
    {
        return $this->hasOne(Effectif::class);
    }

    public function infrastructure()
    {
        return $this->hasOne(Infrastructure::class);
    }

    // Accessors pour faciliter l'accès aux données
    public function getRegionAttribute()
    {
        return $this->localisation?->region;
    }

    public function getPrefectureAttribute()
    {
        return $this->localisation?->prefecture;
    }

    public function getLibelleTypeMilieuAttribute()
    {
        return $this->milieu?->libelle_type_milieu;
    }

    public function getLibelleTypeStatutEtabAttribute()
    {
        return $this->statut?->libelle_type_statut_etab;
    }

    public function getLibelleTypeSystemeAttribute()
    {
        return $this->systeme?->libelle_type_systeme;
    }

    public function getLibelleTypeAnneeAttribute()
    {
        return $this->annee?->libelle_type_annee;
    }
}
