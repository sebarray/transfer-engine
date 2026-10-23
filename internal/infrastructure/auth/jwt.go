package auth

import (
	"context"
	"crypto/ed25519"
	"github.com/golang-jwt/jwt/v5"
	"strings"
	"transfer-engine/internal/business/identity"
	"unicode/utf8"
)

type JWT struct {
	key    ed25519.PublicKey
	parser *jwt.Parser
}

func NewJWT(key []byte, issuer, audience string) (*JWT, error) {
	if len(key) != ed25519.PublicKeySize || strings.TrimSpace(issuer) == "" || strings.TrimSpace(audience) == "" {
		return nil, identity.ErrUnauthenticated
	}
	return &JWT{key: append(ed25519.PublicKey(nil), key...), parser: jwt.NewParser(jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer(issuer), jwt.WithAudience(audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt())}, nil
}
func (j *JWT) Authenticate(ctx context.Context, raw string) (string, error) {
	if ctx.Err() != nil || len(raw) == 0 || len(raw) > 8192 {
		return "", identity.ErrUnauthenticated
	}
	claims := &jwt.RegisteredClaims{}
	token, err := j.parser.ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) { return j.key, nil })
	if err != nil || !token.Valid || strings.TrimSpace(claims.Subject) == "" || len(claims.Subject) > 200 || !utf8.ValidString(claims.Subject) || strings.ContainsRune(claims.Subject, 0) {
		return "", identity.ErrUnauthenticated
	}
	return claims.Subject, nil
}
