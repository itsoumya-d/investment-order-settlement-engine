package pkg

import "testing"

func TestInit(t *testing.T) {
	if 1+1 != 2 {
		t.Fatal("math failed")
	}
}
