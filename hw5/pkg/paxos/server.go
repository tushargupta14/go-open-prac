package paxos

import (
	"coms4113/hw5/pkg/base"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	//TODO: implement it

	//newNodes := make([]base.Node, 0, 3)
	prepmsg, ok := message.(*ProposeRequest)
	var flag bool
	if ok {
		// Propose Phase
		newNode := server.copy()
		if prepmsg.N > newNode.n_p {
			newNode.n_p = prepmsg.N
			flag = true

		} else {
			flag = false
		}
		res := &ProposeResponse{
			CoreMessage: base.MakeCoreMessage(prepmsg.To(), prepmsg.From()),
			Ok:          flag,
			N_p:         newNode.n_p,
			N_a:         newNode.n_a,
			V_a:         newNode.v_a,
			SessionId:   prepmsg.SessionId,
		}
		newNode.SetSingleResponse(res)
		return []base.Node{newNode}
	}

	proposeresponse, ok := message.(*ProposeResponse)
	if ok {
		// Propose Response
		// index of the peer which sent the response
		var peer_idx int
		for idx, peer := range server.peers {
			if proposeresponse.From() == peer {
				peer_idx = idx
				break
			}
		}

		newNodes := make([]base.Node, 0, 3)

		if server.proposer.Responses[peer_idx] {
			// Already given reponse
			return []base.Node{server}
		}
		// Update responses
		newNode := server.copy()
		newNode.proposer.ResponseCount++
		newNode.proposer.Responses[peer_idx] = true

		if proposeresponse.N_p > newNode.n_p {
			newNode.n_p = proposeresponse.N_p
		}
		if proposeresponse.Ok {
			newNode.proposer.SuccessCount++
			if proposeresponse.N_a > newNode.proposer.N_a_max {
				newNode.proposer.N_a_max = proposeresponse.N_a
				newNode.proposer.V = proposeresponse.V_a
			}
		}

		if newNode.proposer.SuccessCount >= len(server.peers)/2+1 {
			// Make a subnode to move to Accept Phase
			acceptNode := newNode.copy()
			acceptNode.proposer.Responses = make([]bool, len(server.peers))
			acceptNode.proposer.ResponseCount = 0
			acceptNode.proposer.SuccessCount = 0
			acceptNode.proposer.Phase = Accept

			messages := make([]base.Message, 0, 3)
			for _, peer := range newNode.peers {
				req := &AcceptRequest{
					CoreMessage: base.MakeCoreMessage(server.Address(), peer),
					N:           newNode.proposer.N,
					V:           newNode.proposer.V,
					SessionId:   newNode.proposer.SessionId,
				}
				messages = append(messages, req)
			}
			acceptNode.SetResponse(messages)
			newNodes = append(newNodes, acceptNode)
		}

		newNodes = append(newNodes, newNode)
		return newNodes
	}
	acceptmsg, ok := message.(*AcceptRequest)
	if ok {
		// Accept phase

		newNode := server.copy()
		if acceptmsg.N >= newNode.n_p {
			newNode.n_p = acceptmsg.N
			newNode.n_a = acceptmsg.N
			newNode.v_a = acceptmsg.V
			flag = true

		} else {
			flag = false
		}
		//fmt.Println("---------------------------Here-----------------------------")
		message := make([]base.Message, 0, 1)
		res := &AcceptResponse{
			CoreMessage: base.MakeCoreMessage(acceptmsg.To(), acceptmsg.From()),
			Ok:          flag,
			N_p:         newNode.n_p,
			SessionId:   acceptmsg.SessionId,
		}
		message = append(message, res)
		newNode.SetResponse(message)
		return []base.Node{newNode}

	}

	acceptresponse, ok := message.(*AcceptResponse)
	if ok {
		// Accept reply
		var peer_idx int
		for idx, peer := range server.peers {
			if acceptresponse.From() == peer {
				peer_idx = idx
				break
			}
		}
		if server.proposer.Responses[peer_idx] {
			// Already given reponse
			return []base.Node{server}
		}
		newNodes := make([]base.Node, 0, 3)

		// Update responses
		newNode := server.copy()
		newNode.proposer.ResponseCount++
		newNode.proposer.Responses[peer_idx] = true

		if acceptresponse.N_p > newNode.n_p {
			newNode.n_p = acceptresponse.N_p
		}
		if acceptresponse.Ok {
			newNode.proposer.SuccessCount++
		}

		if newNode.proposer.SuccessCount >= len(server.peers)/2+1 {
			// Make a subnode to move to Decide Phase
			decideNode := newNode.copy()
			decideNode.proposer.Responses = make([]bool, len(server.peers))
			decideNode.proposer.ResponseCount = 0
			decideNode.proposer.SuccessCount = 0
			decideNode.proposer.Phase = Decide
			decideNode.agreedValue = newNode.proposer.V
			messages := make([]base.Message, 0, 3)
			for _, peer := range newNode.peers {
				req := &DecideRequest{
					CoreMessage: base.MakeCoreMessage(server.Address(), peer),
					V:           newNode.proposer.V,
					SessionId:   newNode.proposer.SessionId,
				}
				messages = append(messages, req)
			}
			decideNode.SetResponse(messages)
			newNodes = append(newNodes, decideNode)
		}

		newNodes = append(newNodes, newNode)
		return newNodes
	}

	decidemsg, ok := message.(*DecideRequest)
	if ok {
		// Decide phase
		newNode := server.copy()
		newNode.agreedValue = decidemsg.V
		//newNodes := make([]base.Node, 0, 1)
		//messages := make([]base.Message, 0, 3)
		//for _, peer := range newNode.peers {
		//	req := &DecideResponse{
		//		CoreMessage: base.MakeCoreMessage(server.Address(), peer),
		//		Ok:          true,
		//		SessionId:   newNode.proposer.SessionId,
		//	}
		//	messages = append(messages, req)
		//}
		//newNode.SetResponse(messages)
		//newNodes = append(newNodes, newNode)
		return []base.Node{newNode}
	}
	return nil
}

// To start a new round of Paxos.
func (server *Server) StartPropose() {
	//TODO: implement it
	messages := make([]base.Message, 0, 3)
	//server.n_p = 0
	server.proposer.V = server.proposer.InitialValue
	server.proposer.Phase = Propose
	server.proposer.N = server.n_p + 1
	server.proposer.SessionId++
	server.proposer.SuccessCount = 0
	server.proposer.ResponseCount = 0
	for _, peer := range server.peers {
		proposemsg := &ProposeRequest{
			CoreMessage: base.MakeCoreMessage(server.peers[server.me], peer),
			N:           server.proposer.N,
			SessionId:   server.proposer.SessionId,
		}
		messages = append(messages, proposemsg)
	}
	server.SetResponse(messages)
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}
