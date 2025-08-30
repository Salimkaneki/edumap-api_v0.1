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
            $table->decimal('latitude', 10, 8)->nullable();
            $table->decimal('longitude', 11, 8)->nullable();

            // Foreign keys vers les tables de référence
            $table->foreignId('localisation_id')->nullable()->constrained('localisations')->onDelete('set null');
            $table->foreignId('milieu_id')->constrained('milieux')->onDelete('cascade');
            $table->foreignId('statut_id')->constrained('statuts')->onDelete('cascade');
            $table->foreignId('systeme_id')->constrained('systemes')->onDelete('cascade');
            $table->foreignId('annee_id')->nullable()->constrained('annees')->onDelete('set null');
            
            // Index pour optimiser les recherches géographiques
            $table->index(['latitude', 'longitude']);
            $table->index('localisation_id');
            
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