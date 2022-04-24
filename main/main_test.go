package main

import (
	"testing"

	"github.com/lincx-911/lincxrpc/codec"
)

func BenchmarkMakeCallGOB(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		MakeCall(codec.GobType)
	}
	b.StopTimer()
}

func BenchmarkMakeCallMSGP(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MakeCall(codec.MessagePackType)
	}
	b.StopTimer()
}
