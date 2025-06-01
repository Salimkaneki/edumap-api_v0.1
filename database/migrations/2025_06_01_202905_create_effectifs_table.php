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
        Schema::create('effectifs', function (Blueprint $table) {
            $table->id();
            $table->foreignId('etablissement_id')->constrained('etablissements')->onDelete('cascade');
            $table->integer('sommedenb_eff_g')->default(0);
            $table->integer('sommedenb_eff_f')->default(0);
            $table->integer('tot')->default(0);
            $table->integer('sommedenb_ens_h')->default(0);
            $table->integer('sommedenb_ens_f')->default(0);
            $table->integer('total_ense')->default(0);
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('effectifs');
    }
};
