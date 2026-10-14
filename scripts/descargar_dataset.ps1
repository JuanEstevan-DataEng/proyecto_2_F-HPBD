# Versión para Windows (PowerShell) de scripts/descargar_dataset.sh
# Descarga los Parquet de StackOverflow que se suben A MANO a Bronze:
# posts 2020, posts 2021 y votes 2020. votes 2021 lo trae el DAG con dlt.
#
# Uso (desde la raíz del proyecto, en PowerShell):
#   powershell -ExecutionPolicy Bypass -File scripts\descargar_dataset.ps1 [carpeta_destino]
# La carpeta por defecto es DATOS_LOCALES del .env (o .\datos).
# Pesos aproximados: posts_2020 3,1 GB · posts_2021 2,6 GB · votes_2020 184 MB.

param([string]$Carpeta)

$ErrorActionPreference = "Stop"
$BaseUrl = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet"

if (-not $Carpeta) {
    $Carpeta = ".\datos"
    if (Test-Path ".env") {
        $linea = Select-String -Path ".env" -Pattern "^DATOS_LOCALES=" | Select-Object -First 1
        if ($linea) { $Carpeta = $linea.Line.Split("=", 2)[1].Trim() }
    }
}
$Destino = Join-Path $Carpeta "raw"
New-Item -ItemType Directory -Force -Path $Destino | Out-Null

foreach ($archivo in @("posts/2020", "posts/2021", "votes/2020")) {
    $salida = Join-Path $Destino (($archivo -replace "/", "_") + ".parquet")   # posts/2020 -> posts_2020.parquet
    Write-Host ">> $BaseUrl/$archivo.parquet -> $salida"
    # curl.exe viene con Windows 10/11; -C - reanuda la descarga si se cortó
    & curl.exe -fL -C - -o $salida "$BaseUrl/$archivo.parquet"
    if ($LASTEXITCODE -ne 0) { throw "Falló la descarga de $archivo" }
}

Get-ChildItem $Destino | Format-Table Name, @{n="MB"; e={[math]::Round($_.Length / 1MB)}}
