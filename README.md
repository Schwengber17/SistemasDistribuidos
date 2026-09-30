# T1 - DiMEx + Snapshot (Sistemas Distribuídos)

## Componentes

Francisco Freitas
Gabriel Ottonelli

## Estrutura

| Arquivo | Conteúdo |
|---|---|
| `PP2PLink/PP2PLink.go` | Perfect Point-to-Point Link sobre TCP (template, sem alterações) |
| `DIMEX/DIMEX-Template.go` | Exclusão mútua distribuída (Ricart-Agrawala) + snapshot (Chandy-Lamport) |
| `useDIMEX-f.go` | Aplicação de teste: acessa `mxOUT.txt` escrevendo `\|` e `.`; o processo 0 inicia um snapshot a cada 100 ms |
| `snapcheck/snapcheck.go` | Ferramenta que lê os snapshots e testa as invariantes |
| `executar.ps1` | Script (Windows) que roda os 3 processos, para e faz as verificações |

## Como executar

Requer Go 1.18+. Todos os comandos rodam dentro desta pasta.

### Opção 1: script (Windows / PowerShell)

```
powershell -ExecutionPolicy Bypass -File .\executar.ps1 -Segundos 60
```

O script apaga os resultados anteriores, compila, sobe os 3 processos, espera, encerra os processos, conta `||` e `..` em `mxOUT.txt` e roda o `snapcheck`.

### Opção 2: manual (3 terminais)

Os três processos precisam ser iniciados em até 3 segundos um do outro, por isso é melhor compilar antes:

```
go build -o dimex useDIMEX-f.go
./dimex 0 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
./dimex 1 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
./dimex 2 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
```

Encerre com Ctrl+C e depois rode:

```
go run ./snapcheck 3
```

Apague `mxOUT.txt` antes de cada execução, porque a aplicação só acrescenta ao arquivo.

## Saídas

- `mxOUT.txt`: deve conter apenas `|.|.|.|.` e nenhum `||` ou `..`.
- `snapshot-p<id>.txt`: um snapshot por linha (JSON) com o id do snapshot, as variáveis do processo (`st`, `waiting`, `lcl`, `reqTs`, `nbrResps`) e o estado dos canais de entrada.

## Invariantes verificadas

1. No máximo um processo em `inMX`.
2. Se todos estão em `noMX`, nenhum `waiting` está marcado e não há mensagens em trânsito.
3. Se `q` está em `waiting` de `p`, então `p` está em `wantMX` ou `inMX` e `q` está em `wantMX`.
4. Se `q` está em `wantMX`: respostas recebidas + `respOK` em trânsito para `q` + `reqEntry` de `q` em trânsito + flags `waiting` para `q` = N-1.
5. Se `q` está em `inMX`: recebeu N-1 respostas e não há pendências (flags, `respOK` ou `reqEntry` em trânsito) envolvendo `q`.
6. Se `p` está em `wantMX` com `q` em `waiting`, então o pedido de `p` vem antes do de `q` (timestamp, desempate por id).

## Injeção de falhas

Troque a constante `FALHA` no início de `DIMEX/DIMEX-Template.go`:

| FALHA | Efeito | Detecção |
|---|---|---|
| 0 | algoritmo correto | nenhuma violação |
| 1 | responde `respOK` mesmo estando na SC | `\|\|` no `mxOUT.txt` e violações da Inv1 |
| 2 | nunca responde enquanto quer a SC | o sistema trava e os snapshots violam a Inv6 |
