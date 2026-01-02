package mqttv5

import (
	"errors"
	"strings"
)

const (
	namespaceDelimiter = "||"

	// DefaultNamespace is the default namespace used when no namespace is specified.
	DefaultNamespace = "default"
)

// Namespace validation errors.
var (
	ErrNamespaceEmpty         = errors.New("namespace cannot be empty")
	ErrNamespaceTooLong       = errors.New("namespace exceeds 253 characters")
	ErrNamespaceLabelEmpty    = errors.New("namespace label cannot be empty")
	ErrNamespaceLabelTooLong  = errors.New("namespace label exceeds 63 characters")
	ErrNamespaceInvalidChar   = errors.New("namespace contains invalid characters")
	ErrNamespaceInvalidFormat = errors.New("namespace label cannot start or end with hyphen")
)

// ValidateNamespace validates that a namespace follows domain name rules:
//   - Cannot be empty
//   - Only lowercase letters (a-z), digits (0-9), hyphens (-), and dots (.)
//   - Labels separated by dots, each 1-63 characters
//   - Labels cannot start or end with hyphen
//   - Total length max 253 characters
func ValidateNamespace(namespace string) error {
	if namespace == "" {
		return ErrNamespaceEmpty
	}
	if len(namespace) > 253 {
		return ErrNamespaceTooLong
	}

	for label := range strings.SplitSeq(namespace, ".") {
		if len(label) == 0 {
			return ErrNamespaceLabelEmpty
		}
		if len(label) > 63 {
			return ErrNamespaceLabelTooLong
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return ErrNamespaceInvalidFormat
		}
		for _, c := range label {
			isLowerAlpha := c >= 'a' && c <= 'z'
			isDigit := c >= '0' && c <= '9'
			isHyphen := c == '-'
			if !isLowerAlpha && !isDigit && !isHyphen {
				return ErrNamespaceInvalidChar
			}
		}
	}
	return nil
}

// NamespaceKey creates a composite key from namespace and id.
func NamespaceKey(namespace, id string) string {
	return namespace + namespaceDelimiter + id
}

// ParseNamespaceKey splits a composite key into namespace and id.
func ParseNamespaceKey(key string) (namespace, id string) {
	parts := strings.SplitN(key, namespaceDelimiter, 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", key
}
