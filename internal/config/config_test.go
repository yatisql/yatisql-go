package config

import (
	"testing"
)

func TestParseDelimiter(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      rune
		wantErr   bool
	}{
		{"comma lowercase", "comma", ',', false},
		{"comma uppercase", "COMMA", ',', false},
		{"csv", "csv", ',', false},
		{"tab lowercase", "tab", '\t', false},
		{"tab uppercase", "TAB", '\t', false},
		{"tsv", "tsv", '\t', false},
		{"auto lowercase", "auto", 0, false},
		{"auto uppercase", "AUTO", 0, false},
		{"invalid", "semicolon", 0, true},
		{"empty", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDelimiter(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDelimiter(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseDelimiter(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid with input files",
			config: Config{
				InputFiles: []string{"data.csv"},
			},
			wantErr: false,
		},
		{
			name: "valid with query",
			config: Config{
				SQLQuery: "SELECT * FROM data",
			},
			wantErr: false,
		},
		{
			name: "valid with both",
			config: Config{
				InputFiles: []string{"data.csv"},
				SQLQuery:   "SELECT * FROM data",
			},
			wantErr: false,
		},
		{
			name:    "invalid empty",
			config:  Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

