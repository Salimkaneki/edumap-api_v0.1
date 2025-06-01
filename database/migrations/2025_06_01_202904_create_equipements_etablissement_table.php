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
        Schema::create('equipements_etablissement', function (Blueprint $table) {
            $table->id();
            $table->foreignId('etablissement_id')->constrained('etablissements')->onDelete('cascade');
            $table->boolean('existe_elect')->default('false');
            $table->boolean('existe_latrine')->default('false');
            $table->boolean('existe_latrine_fonct')->default('false');
            $table->boolean('acces_toute_saison')->default('false');
            $table->boolean('eau')->default('false');
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('equipements_etablissement');
    }
};
