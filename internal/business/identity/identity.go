package identity

import (
	"context"
	"errors"
)

var ErrUnauthenticated = errors.New("invalid authentication")

type Authenticator interface {
	Authenticate(context.Context, string) (string, error)
}
