package fixture

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type jwtClaims struct {
	Audience string `json:"aud"`
	Subject  string `json:"sub"`
	TenantID string `json:"tid"`
	Expires  int64  `json:"exp"`
	IssuedAt int64  `json:"iat"`
	Fitz     struct {
		Permissions []string `json:"permissions"`
	} `json:"fitz"`
}

func GenerateValidTestJWT(secret string, audience string) (string, error) {
	return generateTestJWT(secret, audience, time.Now().Add(time.Hour), defaultPermissions())
}

func GenerateExpiredTestJWT(secret string, audience string) (string, error) {
	return generateTestJWT(secret, audience, time.Now().Add(-time.Hour), defaultPermissions())
}

func GenerateInvalidSignatureTestJWT(secret string, audience string) (string, error) {
	return generateTestJWT(secret+"-invalid", audience, time.Now().Add(time.Hour), defaultPermissions())
}

func GenerateScopedTestJWT(secret string, audience string, permissions []string) (string, error) {
	return generateTestJWT(secret, audience, time.Now().Add(time.Hour), permissions)
}

func generateTestJWT(secret string, audience string, expiresAt time.Time, permissions []string) (string, error) {
	now := time.Now().Unix()
	claims := jwtClaims{
		Audience: audience,
		Subject:  "fitz-go-tests",
		TenantID: "fitz-go-tests",
		Expires:  expiresAt.Unix(),
		IssuedAt: now,
	}
	claims.Fitz.Permissions = permissions

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss":  "",
		"aud":  claims.Audience,
		"sub":  claims.Subject,
		"tid":  claims.TenantID,
		"exp":  claims.Expires,
		"iat":  claims.IssuedAt,
		"fitz": map[string]any{"permissions": claims.Fitz.Permissions},
	})
	return token.SignedString([]byte(secret))
}

func defaultPermissions() []string {
	return []string{
		"kv://**#*",
		"queue://**#*",
		"notice://**#*",
		"stream://**#*",
		"rpc://**#*",
		"lease://**#*",
		"schedule://**#*",
	}
}
