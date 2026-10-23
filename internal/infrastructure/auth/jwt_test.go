package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"github.com/golang-jwt/jwt/v5"
	"testing"
	"time"
)

func TestJWTTrustBoundary(t *testing.T) {
	pub, private, _ := ed25519.GenerateKey(rand.Reader)
	_, wrong, _ := ed25519.GenerateKey(rand.Reader)
	validator, err := NewJWT(pub, "issuer", "audience")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"valid", "expired", "no expiry", "issuer", "audience", "subject", "not before", "issued future", "signature", "algorithm", "none"} {
		t.Run(name, func(t *testing.T) {
			claims := jwt.RegisteredClaims{Issuer: "issuer", Subject: "alice", Audience: jwt.ClaimStrings{"audience"}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}
			method := jwt.SigningMethod(jwt.SigningMethodEdDSA)
			var key any = private
			switch name {
			case "expired":
				claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Hour))
			case "no expiry":
				claims.ExpiresAt = nil
			case "issuer":
				claims.Issuer = "other"
			case "audience":
				claims.Audience = nil
			case "subject":
				claims.Subject = ""
			case "not before":
				claims.NotBefore = jwt.NewNumericDate(time.Now().Add(time.Hour))
			case "issued future":
				claims.IssuedAt = jwt.NewNumericDate(time.Now().Add(time.Hour))
			case "signature":
				key = wrong
			case "algorithm":
				method = jwt.SigningMethodHS256
				key = []byte(pub)
			case "none":
				method = jwt.SigningMethodNone
				key = jwt.UnsafeAllowNoneSignatureType
			}
			raw, err := jwt.NewWithClaims(method, claims).SignedString(key)
			if err != nil {
				t.Fatal(err)
			}
			sub, err := validator.Authenticate(context.Background(), raw)
			if name == "valid" {
				if err != nil || sub != "alice" {
					t.Fatal("valid token rejected")
				}
			} else if err == nil || sub != "" {
				t.Fatal("invalid token accepted")
			}
		})
	}
}
