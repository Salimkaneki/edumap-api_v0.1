<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::create('etablissements', function (Blueprint $table) {
            $table->id();
            $table->string('code_etablissement')->unique();
            $table->string('nom_etablissement');
            $table->string('region');
            $table->string('prefecture'); 
            $table->string('canton_village_autonome');
            $table->string('ville_village_quartier');
            $table->string('libelle_type_milieu');
            $table->string('libelle_type_statut_etab');
            $table->string('libelle_type_systeme');
            $table->boolean('existe_elect')->default(false);
            $table->boolean('existe_latrine')->default(false);
            $table->boolean('existe_latrine_fonct')->default(false);
            $table->boolean('acces_toute_saison')->default(false);
            $table->boolean('eau')->default(false);
            $table->string('latitude', 20);
            $table->string('longitude', 20);
            $table->integer('sommedenb_eff_g')->default(0);
            $table->integer('sommedenb_eff_f')->default(0);
            $table->integer('tot')->default(0);
            $table->integer('sommedenb_ens_h')->default(0);
            $table->integer('sommedenb_ens_f')->default(0);
            $table->integer('total_ense')->default(0);
            $table->integer('sommedenb_salles_classes_dur')->default(0);
            $table->integer('sommedenb_salles_classes_banco')->default(0);
            $table->integer('sommedenb_salles_classes_autre')->default(0);
            $table->string('libelle_type_annee')->nullable();
            $table->string('commune_etab')->nullable();
            
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('etablissements');
    }
};