package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

// Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
func ToA2RejectP1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Propose {
			for _, m := range s.Network {
				resp, ok := m.(*ProposeResponse)
				if ok && resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 got OK in Propose Phase")
		}
		return valid

	}

	p2PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		valid := false
		otheroks := 0
		if s3.proposer.Phase == Propose {
			for _, m := range s.Network {
				resp, ok := m.(*ProposeResponse)
				if ok && resp.Ok && m.To() == s3.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p2 got OK in Propose Phase")
		}
		return valid
	}

	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 got Reject in Accept Phase")
		}
		return valid
	}
	return []func(s *base.State) bool{p1PreparePhase, p2PreparePhase, p1AcceptPhase}
}

// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	p2AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		valid := false
		otheroks := 0
		if s3.proposer.Phase == Accept {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && resp.Ok && m.To() == s3.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... P3 got Accept in Accept Phase")
		}
		return valid
	}
	return []func(s *base.State) bool{p2AcceptPhase}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {

	p1PreparePhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s1.proposer.Phase == Propose && s1.proposer.ResponseCount >= 2 && s3.proposer.Phase == "" {
			valid = true
		}
		if valid {
			fmt.Println("... P1 got OK in Propose Phase")
		}
		return valid

	}

	p3PreparePhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s3.proposer.Phase == Propose && s3.proposer.ResponseCount >= 2 && s3.proposer.SuccessCount >= 1 {
			valid = true
		}
		if valid {
			fmt.Println("... P3 got OK in Propose Phase")
		}
		return valid
	}

	p1AcceptPhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		//s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 1 {
			valid = true
		}
		if valid {
			fmt.Println("... P1 entered Accept Phase")
		}
		return valid

	}
	return []func(s *base.State) bool{p1PreparePhase, p3PreparePhase, p1AcceptPhase}
	//panic("fill me in")
}

// Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {

	p1PreparePhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		//s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s1.proposer.Phase == Propose {
			valid = true
		}
		if valid {
			fmt.Println("... P1 enters in Propose Phase Again")
		}
		return valid
	}

	p1PreparePhase2 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		//s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s1.proposer.Phase == Propose && s1.proposer.ResponseCount >= 1 {
			valid = true
		}
		if valid {
			fmt.Println("... P1 got OK in Propose Phase Again")
		}
		return valid
	}

	p3AcceptPhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		//s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s3.proposer.Phase == Accept && s3.proposer.ResponseCount >= 2 {
			valid = true
		}
		if valid {
			fmt.Println("... P3 entered Accept Phase")
		}
		return valid

	}
	return []func(s *base.State) bool{p1PreparePhase, p1PreparePhase2, p3AcceptPhase}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	p3PreparePhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s3 := s.Nodes()["s3"].(*Server)
		//s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s3.proposer.Phase == Propose {
			valid = true
		}
		if valid {
			fmt.Println("... P3 enters in Propose Phase Again")
		}
		return valid
	}

	p3PreparePhase2 := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s3 := s.Nodes()["s3"].(*Server)
		//s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s3.proposer.Phase == Propose && s3.proposer.ResponseCount >= 1 {
			valid = true
		}
		if valid {
			fmt.Println("... P3 got OK in Propose Phase Again")
		}
		return valid
	}

	p1AcceptPhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		//s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s1.proposer.Phase == Accept && s1.proposer.ResponseCount >= 2 {
			valid = true
		}
		if valid {
			fmt.Println("... P3 entered Accept Phase")
		}
		return valid

	}
	return []func(s *base.State) bool{p3PreparePhase, p3PreparePhase2, p1AcceptPhase}
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {

	p1PreparePhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s1.proposer.Phase == Propose && s1.proposer.ResponseCount >= 2 && s3.proposer.Phase == "" {
			valid = true
		}
		if valid {
			fmt.Println("... P1 got OK in Propose Phase")
		}
		return valid
	}

	p3PreparePhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		//s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s3.proposer.Phase == Propose && s3.proposer.ResponseCount >= 1 && s3.proposer.SuccessCount >= 1 {
			valid = true
		}
		if valid {
			fmt.Println("... P3 got OK in Propose Phase")
		}
		return valid

	}

	p1AcceptPhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		//s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s1.proposer.Phase == Accept && s1.proposer.ResponseCount >= 1 {
			valid = true
		}
		if valid {
			fmt.Println("... P1 entered Accept Phase")
		}
		return valid

	}

	return []func(s *base.State) bool{p1PreparePhase, p3PreparePhase, p1AcceptPhase}
}

// Fill in the function to lead the program continue  P3's proposal  and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {
	p3AcceptPhase := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		//s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s3.proposer.Phase == Accept && s3.proposer.ResponseCount >= 1 && s3.proposer.SuccessCount >= 1 {
			valid = true
		}
		if valid {
			fmt.Println("... P3 enter Accept Phase")
		}
		return valid

	}
	return []func(s *base.State) bool{p3AcceptPhase}
}
