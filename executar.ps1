# Roda 3 processos DIMEX por alguns segundos, depois verifica mxOUT.txt.
# Uso:  .\executar.ps1            (60 segundos)
#       .\executar.ps1 -Segundos 30
param([int]$Segundos = 60)

$addrs = "127.0.0.1:5000", "127.0.0.1:6001", "127.0.0.1:7002"

Remove-Item mxOUT.txt, saida-p*.txt -ErrorAction SilentlyContinue
go build -o dimex.exe useDIMEX-f.go
if (-not $?) { exit 1 }

$procs = foreach ($i in 0..2) {
    Start-Process .\dimex.exe -ArgumentList (@($i) + $addrs) -PassThru -NoNewWindow -RedirectStandardOutput "saida-p$i.txt"
}
Write-Host "3 processos rodando por $Segundos s (saida de cada um em saida-p<id>.txt) ..."
Start-Sleep -Seconds $Segundos
$procs | Stop-Process -Force

$mx = Get-Content mxOUT.txt -Raw
Write-Host "`n--- mxOUT.txt ---"
Write-Host "acessos a SC: $(([regex]::Matches($mx, '\|')).Count)"
Write-Host "ocorrencias de '||': $(([regex]::Matches($mx, '\|\|')).Count)"
Write-Host "ocorrencias de '..': $(([regex]::Matches($mx, '\.\.')).Count)"
