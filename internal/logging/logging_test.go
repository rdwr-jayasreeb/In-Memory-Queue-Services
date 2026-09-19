package logging

import "testing"

func TestValidateLevel(t *testing.T) {
	for _, value := range []string{"DEBUG", "INFO", "WARN", "ERROR", "debug"} {
		if err := ValidateLevel(value); err != nil {
			t.Fatalf("ValidateLevel(%q) returned %v", value, err)
		}
	}

	for _, value := range []string{"", "TRACE", "warning", "invalid"} {
		if err := ValidateLevel(value); err == nil {
			t.Fatalf("ValidateLevel(%q) did not return an error", value)
		}
	}
}
