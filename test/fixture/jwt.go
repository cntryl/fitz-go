package fixture

import (
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type jwtClaims struct {
	Audience    string   `json:"aud"`
	Subject     string   `json:"sub"`
	TenantID    string   `json:"tid"`
	Expires     int64    `json:"exp"`
	IssuedAt    int64    `json:"iat"`
	Permissions []string `json:"permissions"`
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
	tenantID := os.Getenv("FITZ_BROKER_JWT_TENANT")
	if tenantID == "" {
		tenantID = "dev"
	}
	claims := jwtClaims{
		Audience:    audience,
		Subject:     "fitz-ts-tests",
		TenantID:    tenantID,
		Expires:     expiresAt.Unix(),
		IssuedAt:    now,
		Permissions: permissions,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss":         "",
		"aud":         claims.Audience,
		"sub":         claims.Subject,
		"tid":         claims.TenantID,
		"exp":         claims.Expires,
		"iat":         claims.IssuedAt,
		"permissions": claims.Permissions,
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
