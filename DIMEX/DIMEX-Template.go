/*  Construido como parte da disciplina: FPPD - PUCRS - Escola Politecnica
    Professor: Fernando Dotti  (https://fldotti.github.io/)
    Modulo representando Algoritmo de Exclusão Mútua Distribuída:
    Semestre 2023/1
	Aspectos a observar:
	   mapeamento de módulo para estrutura
	   inicializacao
	   semantica de concorrência: cada evento é atômico
	   							  módulo trata 1 por vez
	Q U E S T A O
	   Além de obviamente entender a estrutura ...
	   Implementar o núcleo do algoritmo ja descrito, ou seja, o corpo das
	   funcoes reativas a cada entrada possível:
	   			handleUponReqEntry()  // recebe do nivel de cima (app)
				handleUponReqExit()   // recebe do nivel de cima (app)
				handleUponDeliverRespOk(msgOutro)   // recebe do nivel de baixo
				handleUponDeliverReqEntry(msgOutro) // recebe do nivel de baixo
*/

package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// injecao de falhas (parte 2, etapa 4):
// 0 = correto; 1 = responde mesmo estando na SC (viola a SC); 2 = nunca responde se quer a SC (bloqueia)
const FALHA = 0

// ------------------------------------------------------------------------------------
// ------- principais tipos
// ------------------------------------------------------------------------------------

type State int // enumeracao dos estados possiveis de um processo
const (
	noMX State = iota
	wantMX
	inMX
)

var stateNames = []string{"noMX", "wantMX", "inMX"}

type dmxReq int // enumeracao dos estados possiveis de um processo
const (
	ENTER dmxReq = iota
	EXIT
	SNAPSHOT // app pede para este processo iniciar um novo snapshot
)

type dmxResp struct { // mensagem do módulo DIMEX infrmando que pode acessar - pode ser somente um sinal (vazio)
	// mensagem para aplicacao indicando que pode prosseguir
}

type DIMEX_Module struct {
	Req       chan dmxReq  // canal para receber pedidos da aplicacao (REQ e EXIT)
	Ind       chan dmxResp // canal para informar aplicacao que pode acessar
	addresses []string     // endereco de todos, na mesma ordem
	id        int          // identificador do processo - é o indice no array de enderecos acima
	st        State        // estado deste processo na exclusao mutua distribuida
	waiting   []bool       // processos aguardando tem flag true
	lcl       int          // relogio logico local
	reqTs     int          // timestamp local da ultima requisicao deste processo
	nbrResps  int
	dbg       bool

	snapId   int               // ultimo snapshot iniciado por este processo
	snaps    map[int]*Snapshot // snapshots em andamento neste processo, por identificador
	snapFile *os.File          // arquivo onde este processo grava seus snapshots

	Pp2plink *PP2PLink.PP2PLink // acesso aa comunicacao enviar por PP2PLinq.Req  e receber por PP2PLinq.Ind
}

// estado gravado por um processo em um snapshot - uma linha JSON em snapshot-p<id>.txt
type Snapshot struct {
	SnapId   int
	Id       int
	St       string
	Waiting  []bool
	Lcl      int
	ReqTs    int
	NbrResps int
	Channels [][]string // Channels[j]: mensagens de j para este processo que estavam em transito

	recording []bool // canais de entrada ainda sendo gravados (campos minusculos nao vao para o arquivo)
	markers   int    // marcadores recebidos ate agora
}

// ------------------------------------------------------------------------------------
// ------- inicializacao
// ------------------------------------------------------------------------------------

func NewDIMEX(_addresses []string, _id int, _dbg bool) *DIMEX_Module {

	p2p := PP2PLink.NewPP2PLink(_addresses[_id], _dbg)

	dmx := &DIMEX_Module{
		Req: make(chan dmxReq, 1),
		Ind: make(chan dmxResp, 1),

		addresses: _addresses,
		id:        _id,
		st:        noMX,
		waiting:   make([]bool, len(_addresses)),
		lcl:       0,
		reqTs:     0,
		dbg:       _dbg,
		snaps:     make(map[int]*Snapshot),

		Pp2plink: p2p}

	for i := 0; i < len(dmx.waiting); i++ {
		dmx.waiting[i] = false
	}
	dmx.Start()
	dmx.outDbg("Init DIMEX!")
	return dmx
}

// ------------------------------------------------------------------------------------
// ------- nucleo do funcionamento
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) Start() {

	go func() {
		for {
			select {
			case dmxR := <-module.Req: // vindo da  aplicação
				if dmxR == ENTER {
					module.outDbg("app pede mx")
					module.handleUponReqEntry() // ENTRADA DO ALGORITMO

				} else if dmxR == EXIT {
					module.outDbg("app libera mx")
					module.handleUponReqExit() // ENTRADA DO ALGORITMO

				} else if dmxR == SNAPSHOT {
					module.outDbg("app pede snapshot")
					module.handleUponReqSnapshot() // ENTRADA DO ALGORITMO
				}

			case msgOutro := <-module.Pp2plink.Ind: // vindo de outro processo
				//fmt.Printf("dimex recebe da rede: ", msgOutro)
				if strings.Contains(msgOutro.Message, "respOK") {
					module.outDbg("         <<<---- responde! " + msgOutro.Message)
					module.recordInTransit(msgOutro)
					module.handleUponDeliverRespOk(msgOutro) // ENTRADA DO ALGORITMO

				} else if strings.Contains(msgOutro.Message, "reqEntry") {
					module.outDbg("          <<<---- pede??  " + msgOutro.Message)
					module.recordInTransit(msgOutro)
					module.handleUponDeliverReqEntry(msgOutro) // ENTRADA DO ALGORITMO

				} else if strings.Contains(msgOutro.Message, "snapshot") {
					module.outDbg("          <<<---- marcador " + msgOutro.Message)
					module.handleUponDeliverSnapshot(msgOutro) // ENTRADA DO ALGORITMO
				}
			}
		}
	}()
}

// ------------------------------------------------------------------------------------
// ------- tratamento de pedidos vindos da aplicacao
// ------- UPON ENTRY
// ------- UPON EXIT
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponReqEntry() {
	/*
					upon event [ dmx, Entry  |  r ]  do
		    			lts.ts++
		    			myTs := lts
		    			resps := 0
		    			para todo processo p
							trigger [ pl , Send | [ reqEntry, r, myTs ]
		    			estado := queroSC
	*/
	module.lcl++
	module.reqTs = module.lcl
	module.nbrResps = 0
	for i, addr := range module.addresses {
		if i != module.id {
			module.sendToLink(addr, fmt.Sprintf("reqEntry;%d;%d", module.id, module.reqTs), "    ")
		}
	}
	module.st = wantMX
}

func (module *DIMEX_Module) handleUponReqExit() {
	/*
						upon event [ dmx, Exit  |  r  ]  do
		       				para todo [p, r, ts ] em waiting
		          				trigger [ pl, Send | p , [ respOk, r ]  ]
		    				estado := naoQueroSC
							waiting := {}
	*/
	for i, w := range module.waiting {
		if w {
			module.sendToLink(module.addresses[i], fmt.Sprintf("respOK;%d", module.id), "    ")
			module.waiting[i] = false
		}
	}
	module.st = noMX
}

// ------------------------------------------------------------------------------------
// ------- tratamento de mensagens de outros processos
// ------- UPON respOK
// ------- UPON reqEntry
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponDeliverRespOk(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	/*
						upon event [ pl, Deliver | p, [ respOk, r ] ]
		      				resps++
		      				se resps = N
		    				então trigger [ dmx, Deliver | free2Access ]
		  					    estado := estouNaSC

	*/
	module.nbrResps++
	if module.nbrResps == len(module.addresses)-1 {
		module.st = inMX
		module.Ind <- dmxResp{}
	}
}

func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	// outro processo quer entrar na SC
	/*
						upon event [ pl, Deliver | p, [ reqEntry, r, rts ]  do
		     				se (estado == naoQueroSC)   OR
		        				 (estado == QueroSC AND  myTs >  ts)
							então  trigger [ pl, Send | p , [ respOk, r ]  ]
		 					senão
		        				se (estado == estouNaSC) OR
		           					 (estado == QueroSC AND  myTs < ts)
		        				então  postergados := postergados + [p, r ]
		     					lts.ts := max(lts.ts, rts.ts)
	*/
	otherId, otherTs := parseMsg(msgOutro.Message)
	respond := module.st == noMX ||
		(module.st == wantMX && before(otherId, otherTs, module.id, module.reqTs))

	switch FALHA {
	case 1:
		respond = respond || module.st == inMX
	case 2:
		respond = module.st == noMX
	}

	if respond {
		module.sendToLink(module.addresses[otherId], fmt.Sprintf("respOK;%d", module.id), "    ")
	} else {
		module.waiting[otherId] = true
	}
	if otherTs > module.lcl {
		module.lcl = otherTs
	}
}

// ------------------------------------------------------------------------------------
// ------- snapshot (Chandy-Lamport)
// ------- UPON snapshot (pedido da app: inicia um novo snapshot)
// ------- UPON marcador [ snapshot, id, snapId ] vindo de outro processo
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponReqSnapshot() {
	module.snapId++
	module.takeSnapshot(module.snapId)
}

func (module *DIMEX_Module) handleUponDeliverSnapshot(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	otherId, snapId := parseMsg(msgOutro.Message)
	snap, ok := module.snaps[snapId]
	if !ok { // primeiro marcador deste snapshot: grava estado e propaga
		snap = module.takeSnapshot(snapId)
	}
	snap.recording[otherId] = false // canal de otherId encerrado (vazio se foi o primeiro marcador)
	snap.markers++
	if snap.markers == len(module.addresses)-1 { // marcador recebido de todos os canais: terminou
		module.saveSnapshot(snap)
	}
}

// grava o estado local, comeca a gravar todos os canais de entrada e envia marcador a todos
func (module *DIMEX_Module) takeSnapshot(snapId int) *Snapshot {
	n := len(module.addresses)
	snap := &Snapshot{
		SnapId:    snapId,
		Id:        module.id,
		St:        stateNames[module.st],
		Waiting:   make([]bool, n),
		Lcl:       module.lcl,
		ReqTs:     module.reqTs,
		NbrResps:  module.nbrResps,
		Channels:  make([][]string, n),
		recording: make([]bool, n),
	}
	copy(snap.Waiting, module.waiting)
	for i := range module.addresses {
		snap.Channels[i] = []string{}
		snap.recording[i] = i != module.id
	}
	module.snaps[snapId] = snap

	for i, addr := range module.addresses {
		if i != module.id {
			module.sendToLink(addr, fmt.Sprintf("snapshot;%d;%d", module.id, snapId), "    ")
		}
	}
	return snap
}

// mensagem do algoritmo recebida: se o canal de origem esta sendo gravado em algum snapshot, ela estava em transito
func (module *DIMEX_Module) recordInTransit(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	otherId, _ := parseMsg(msgOutro.Message)
	for _, snap := range module.snaps {
		if snap.recording[otherId] {
			snap.Channels[otherId] = append(snap.Channels[otherId], msgOutro.Message)
		}
	}
}

func (module *DIMEX_Module) saveSnapshot(snap *Snapshot) {
	if module.snapFile == nil {
		f, err := os.Create(fmt.Sprintf("snapshot-p%d.txt", module.id))
		if err != nil {
			fmt.Println("Error creating snapshot file:", err)
		}
		module.snapFile = f
	}
	line, _ := json.Marshal(snap)
	module.snapFile.WriteString(string(line) + "\n")
	delete(module.snaps, snap.SnapId)
}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) sendToLink(address string, content string, space string) {
	module.outDbg(space + " ---->>>>   to: " + address + "     msg: " + content)
	module.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
		To:      address,
		Message: content}
}

// mensagens tem o formato "tipo;idRemetente;valor" - retorna idRemetente e valor
func parseMsg(msg string) (int, int) {
	parts := strings.Split(msg, ";")
	from, _ := strconv.Atoi(parts[1])
	val := 0
	if len(parts) > 2 {
		val, _ = strconv.Atoi(parts[2])
	}
	return from, val
}

func before(oneId, oneTs, othId, othTs int) bool {
	if oneTs < othTs {
		return true
	} else if oneTs > othTs {
		return false
	} else {
		return oneId < othId
	}
}

func (module *DIMEX_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . . . . [ DIMEX : " + s + " ]")
	}
}
