package database

import "strings"

// SanitizeColumnName sanitizes a column name for SQL compatibility.
// - Replaces invalid characters with underscores
// - Prefixes with "col_" if the name starts with a digit
// - Returns "unnamed" for empty names
func SanitizeColumnName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "unnamed"
	}

	result := make([]rune, 0, len(name))
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			result = append(result, r)
		} else {
			result = append(result, '_')
		}
	}

	sanitized := string(result)
	if sanitized != "" && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "col_" + sanitized
	}

	return sanitized
}
