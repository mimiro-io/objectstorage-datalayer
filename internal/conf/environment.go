package conf

import (
	"go.uber.org/zap"
)

type Env struct {
	Logger             *zap.SugaredLogger
	Env                string
	Port               string
	ConfigLocation     string
	RefreshInterval    string
	ServiceName        string
	FullsyncChunkSize  int64
	FullsyncTempFolder string
	Auth               *AuthConfig
}

type AuthConfig struct {
	WellKnown     string
	Audience      string
	AudienceAuth0 string
	Issuer        string
	IssuerAuth0   string
	Middleware    string
}
