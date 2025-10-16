<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Export des Établissements - EduMap Togo</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'DejaVu Sans', sans-serif;
            font-size: 9px;
            color: #333;
            line-height: 1.4;
        }
        
        .header {
            text-align: center;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #4472C4;
        }
        
        .header h1 {
            color: #4472C4;
            font-size: 18px;
            margin-bottom: 5px;
        }
        
        .header p {
            font-size: 10px;
            color: #666;
        }
        
        .filters {
            background-color: #f8f9fa;
            padding: 10px;
            margin-bottom: 15px;
            border-left: 4px solid #4472C4;
        }
        
        .filters h3 {
            font-size: 11px;
            margin-bottom: 8px;
            color: #4472C4;
        }
        
        .filter-item {
            display: inline-block;
            margin-right: 15px;
            margin-bottom: 5px;
        }
        
        .filter-item strong {
            color: #333;
        }
        
        .stats {
            display: table;
            width: 100%;
            margin-bottom: 15px;
        }
        
        .stat-box {
            display: table-cell;
            width: 20%;
            padding: 10px;
            text-align: center;
            background-color: #f8f9fa;
            border: 1px solid #ddd;
        }
        
        .stat-box .number {
            font-size: 16px;
            font-weight: bold;
            color: #4472C4;
            display: block;
            margin-bottom: 5px;
        }
        
        .stat-box .label {
            font-size: 9px;
            color: #666;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        
        thead {
            background-color: #4472C4;
            color: white;
        }
        
        thead th {
            padding: 8px 4px;
            text-align: left;
            font-size: 8px;
            font-weight: bold;
            border: 1px solid #3461a8;
        }
        
        tbody td {
            padding: 6px 4px;
            border: 1px solid #ddd;
            font-size: 8px;
        }
        
        tbody tr:nth-child(even) {
            background-color: #f8f9fa;
        }
        
        tbody tr:hover {
            background-color: #e9ecef;
        }
        
        .footer {
            position: fixed;
            bottom: 0;
            width: 100%;
            text-align: center;
            font-size: 8px;
            color: #999;
            padding: 10px 0;
            border-top: 1px solid #ddd;
        }
        
        .page-break {
            page-break-after: always;
        }
        
        .text-center {
            text-align: center;
        }
        
        .text-right {
            text-align: right;
        }
        
        .badge {
            display: inline-block;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 7px;
            font-weight: bold;
        }
        
        .badge-success {
            background-color: #28a745;
            color: white;
        }
        
        .badge-danger {
            background-color: #dc3545;
            color: white;
        }
        
        .badge-info {
            background-color: #17a2b8;
            color: white;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏫 RÉPUBLIQUE TOGOLAISE - EDUMAP</h1>
        <p>Export des Établissements Scolaires</p>
        <p style="font-size: 8px;">Généré le {{ $date }}</p>
    </div>

    @if(!empty($filters['region']) || !empty($filters['prefecture']) || !empty($filters['libelle_type_milieu']) || !empty($filters['libelle_type_statut_etab']) || !empty($filters['libelle_type_systeme']))
    <div class="filters">
        <h3>📋 Filtres appliqués :</h3>
        @if(!empty($filters['region']))
            <div class="filter-item"><strong>Région :</strong> {{ $filters['region'] }}</div>
        @endif
        @if(!empty($filters['prefecture']))
            <div class="filter-item"><strong>Préfecture :</strong> {{ $filters['prefecture'] }}</div>
        @endif
        @if(!empty($filters['libelle_type_milieu']))
            <div class="filter-item"><strong>Milieu :</strong> {{ $filters['libelle_type_milieu'] }}</div>
        @endif
        @if(!empty($filters['libelle_type_statut_etab']))
            <div class="filter-item"><strong>Statut :</strong> {{ $filters['libelle_type_statut_etab'] }}</div>
        @endif
        @if(!empty($filters['libelle_type_systeme']))
            <div class="filter-item"><strong>Système :</strong> {{ $filters['libelle_type_systeme'] }}</div>
        @endif
    </div>
    @endif

    <div class="stats">
        <div class="stat-box">
            <span class="number">{{ $stats['total'] }}</span>
            <span class="label">Établissements</span>
        </div>
        <div class="stat-box">
            <span class="number">{{ number_format($stats['total_eleves']) }}</span>
            <span class="label">Total Élèves</span>
        </div>
        <div class="stat-box">
            <span class="number">{{ number_format($stats['total_enseignants']) }}</span>
            <span class="label">Total Enseignants</span>
        </div>
        <div class="stat-box">
            <span class="number">{{ $stats['avec_electricite'] }}</span>
            <span class="label">Avec Électricité</span>
        </div>
        <div class="stat-box">
            <span class="number">{{ $stats['avec_eau'] }}</span>
            <span class="label">Avec Eau</span>
        </div>
    </div>

    @if(isset($is_limited) && $is_limited)
    <div class="filters" style="background-color: #fff3cd; border-left-color: #ffc107;">
        <p style="color: #856404; font-weight: bold;">⚠️ Attention : Ce PDF affiche les 100 premiers établissements sur {{ $stats['total'] }} au total. Pour exporter toutes les données, utilisez le format Excel ou CSV.</p>
    </div>
    @endif

    <table>
        <thead>
            <tr>
                <th style="width: 12%;">Code</th>
                <th style="width: 28%;">Nom</th>
                <th style="width: 15%;">Région</th>
                <th style="width: 15%;">Préfecture</th>
                <th style="width: 15%;">Milieu</th>
                <th style="width: 15%;">Statut</th>
            </tr>
        </thead>
        <tbody>
            @foreach($etablissements as $etab)
            <tr>
                <td>{{ $etab->code_etablissement ?? 'N/A' }}</td>
                <td>{{ $etab->nom_etablissement ?? 'N/A' }}</td>
                <td>{{ $etab->region ?? 'N/A' }}</td>
                <td>{{ $etab->prefecture ?? 'N/A' }}</td>
                <td>{{ $etab->libelle_type_milieu ?? 'N/A' }}</td>
                <td style="font-size: 7px;">{{ $etab->libelle_type_statut_etab ?? 'N/A' }}</td>
            </tr>
            @endforeach
        </tbody>
    </table>

    <div class="footer">
        <p>EduMap Togo - Système de Gestion des Établissements Scolaires | Page {PAGE_NUM} / {PAGE_COUNT}</p>
        <p>Document généré automatiquement - {{ $date }}</p>
    </div>
</body>
</html>
