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
            $table->string('code_etablissement');
            $table->string('nom_etablissement');
             $table->string('latitude', 10, 8)->nullable();
             $table->string('longitude', 11, 8)->nullable();

            $table->foreignId('localisation_id')->constrained('localisations')->onDelete;
            $table->foreignId('milieu_id')->constrained('milieux')->onDelete('cascade');
            $table->foreignId('statut_id')->constrained('statuts')->onDelete('cascade');
            $table->foreignId('systeme_id')->constrained('systemes')->onDelete('cascade');
            $table->foreignId('annee_id')->nullable()->constrained('annees')->onDelete('set null');
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
