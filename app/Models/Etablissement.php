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

    // Scopes pour faciliter les recherches
    public function scopeNearby($query, $latitude, $longitude, $radius = 10)
    {
        return $query->selectRaw("*, (
            6371 * acos(
                cos(radians(?)) * 
                cos(radians(latitude)) * 
                cos(radians(longitude) - radians(?)) + 
                sin(radians(?)) * 
                sin(radians(latitude))
            )
        ) AS distance", [$latitude, $longitude, $latitude])
        ->having('distance', '<', $radius)
        ->orderBy('distance');
    }

    public function scopeByRegion($query, $region)
    {
        return $query->whereHas('localisation', function($q) use ($region) {
            $q->where('region', $region);
        });
    }

    public function scopeByStatut($query, $statut)
    {
        return $query->whereHas('statut', function($q) use ($statut) {
            $q->where('libelle_type_statut_etab', $statut);
        });
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

    // Méthode pour obtenir toutes les données complètes
    public function getFullDataAttribute()
    {
        return [
            'etablissement' => $this->only(['id', 'code_etablissement', 'nom_etablissement', 'latitude', 'longitude']),
            'localisation' => $this->localisation,
            'caracteristiques' => [
                'milieu' => $this->milieu,
                'statut' => $this->statut,
                'systeme' => $this->systeme,
                'annee' => $this->annee,
            ],
            'effectifs' => $this->effectif,
            'infrastructures' => $this->infrastructure,
            'equipements' => $this->equipement,
        ];
    }
}
