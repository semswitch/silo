package expose

import "testing"

func TestDefaultConfigUsesOneConnection(t *testing.T) {
	if DefaultConfig.NumConnections != 1 {
		t.Fatalf("default NBD connections = %d, want 1", DefaultConfig.NumConnections)
	}
}
