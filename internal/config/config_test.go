package config

import "testing"

func TestClickHouseValidate(t *testing.T) {
	valid := ClickHouse{
		Address:  "clickhouse:9000",
		Database: "beaver",
		Table:    "raw_events",
		Username: "beaver",
		Password: "beaver",
	}

	tests := []struct {
		name    string
		config  ClickHouse
		wantErr string
	}{
		{name: "valid", config: valid},
		{
			name:    "missing username",
			config:  ClickHouse{Address: valid.Address, Database: valid.Database, Table: valid.Table, Password: valid.Password},
			wantErr: "clickhouse.username is required",
		},
		{
			name:    "missing password",
			config:  ClickHouse{Address: valid.Address, Database: valid.Database, Table: valid.Table, Username: valid.Username},
			wantErr: "clickhouse.password is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}

			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("Validate() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
