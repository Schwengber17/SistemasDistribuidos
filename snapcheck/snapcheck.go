// Ferramenta que avalia os snapshots gravados pelo DIMEX.
// Uso:  go run ./snapcheck 3      (le snapshot-p0.txt, snapshot-p1.txt, snapshot-p2.txt)
// Para cada snapshot SnId junta os estados gravados por todos os processos (estado global)
// e testa todas as invariantes. Avisa invariantes violadas e o snapshot.

package main

import (
	DIMEX "SD/DIMEX"
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type globalState []DIMEX.Snapshot // indice = id do processo

var invariants = []struct {
	name string
	test func(globalState) bool
}{
	{"Inv1: no maximo um processo na SC", inv1},
	{"Inv2: se todos estao em noMX, nao ha waiting nem mensagens em transito", inv2},
	{"Inv3: se q esta em waiting de p, entao p esta em wantMX/inMX e q esta em wantMX", inv3},
	{"Inv4: se q esta em wantMX, respostas recebidas + pendencias (respOK/reqEntry em transito, waiting) = N-1", inv4},
	{"Inv5: se q esta em inMX, recebeu N-1 respostas e nao ha pendencias para q", inv5},
	{"Inv6: se p em wantMX tem q em waiting, entao o pedido de p e anterior ao de q", inv6},
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("uso: go run ./snapcheck <numero de processos>")
		return
	}
	n, _ := strconv.Atoi(os.Args[1])

	snaps := map[int]globalState{}
	for i := 0; i < n; i++ {
		f, err := os.Open(fmt.Sprintf("snapshot-p%d.txt", i))
		if err != nil {
			fmt.Println("Error opening file:", err)
			return
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			var s DIMEX.Snapshot
			if json.Unmarshal(sc.Bytes(), &s) != nil {
				continue // linha incompleta (processo interrompido no meio da escrita)
			}
			if snaps[s.SnapId] == nil {
				snaps[s.SnapId] = make(globalState, n)
			}
			snaps[s.SnapId][i] = s
		}
		f.Close()
	}

	ids := []int{}
	for id := range snaps {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	analyzed, incomplete := 0, 0
	violations := make([]int, len(invariants))
	for _, id := range ids {
		gs := snaps[id]
		if !complete(gs) { // algum processo nao chegou a gravar este snapshot
			incomplete++
			continue
		}
		analyzed++
		ok := true
		for k, inv := range invariants {
			if !inv.test(gs) {
				fmt.Printf("snapshot %d VIOLOU %s\n", id, inv.name)
				violations[k]++
				ok = false
			}
		}
		if !ok {
			for _, s := range gs {
				fmt.Printf("    p%d: st=%s waiting=%v reqTs=%d nbrResps=%d canais=%v\n",
					s.Id, s.St, s.Waiting, s.ReqTs, s.NbrResps, s.Channels)
			}
		}
	}

	fmt.Printf("\nsnapshots analisados: %d   (incompletos ignorados: %d)\n", analyzed, incomplete)
	for k, inv := range invariants {
		fmt.Printf("  %-4d violacoes  %s\n", violations[k], inv.name)
	}
}

func complete(gs globalState) bool {
	for _, s := range gs {
		if s.St == "" {
			return false
		}
	}
	return true
}

// numero de mensagens do tipo dado em transito no canal de -> para
func inTransit(gs globalState, from, to int, kind string) int {
	c := 0
	for _, m := range gs[to].Channels[from] {
		if strings.HasPrefix(m, kind) {
			c++
		}
	}
	return c
}

// pendencias do pedido de q junto a p: q ainda nao recebeu a resposta de p
func pending(gs globalState, q, p int) int {
	c := inTransit(gs, q, p, "reqEntry") + inTransit(gs, p, q, "respOK")
	if gs[p].Waiting[q] {
		c++
	}
	return c
}

func inv1(gs globalState) bool {
	count := 0
	for _, s := range gs {
		if s.St == "inMX" {
			count++
		}
	}
	return count <= 1
}

func inv2(gs globalState) bool {
	for _, s := range gs {
		if s.St != "noMX" {
			return true
		}
	}
	for _, s := range gs {
		for j := range gs {
			if s.Waiting[j] || len(s.Channels[j]) > 0 {
				return false
			}
		}
	}
	return true
}

func inv3(gs globalState) bool {
	for p := range gs {
		for q := range gs {
			if gs[p].Waiting[q] && (gs[p].St == "noMX" || gs[q].St != "wantMX") {
				return false
			}
		}
	}
	return true
}

func inv4(gs globalState) bool {
	for q := range gs {
		if gs[q].St != "wantMX" {
			continue
		}
		sum := gs[q].NbrResps
		for p := range gs {
			if p != q {
				sum += pending(gs, q, p)
			}
		}
		if sum != len(gs)-1 {
			return false
		}
	}
	return true
}

func inv5(gs globalState) bool {
	for q := range gs {
		if gs[q].St != "inMX" {
			continue
		}
		if gs[q].NbrResps != len(gs)-1 {
			return false
		}
		for p := range gs {
			if p != q && pending(gs, q, p) > 0 {
				return false
			}
		}
	}
	return true
}

func inv6(gs globalState) bool {
	for p := range gs {
		for q := range gs {
			if gs[p].St == "wantMX" && gs[p].Waiting[q] {
				pFirst := gs[p].ReqTs < gs[q].ReqTs || (gs[p].ReqTs == gs[q].ReqTs && p < q)
				if !pFirst {
					return false
				}
			}
		}
	}
	return true
}
