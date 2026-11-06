package config

import (
	"fmt"
	"net"
	"net/url"
)

// Validate checks the loaded configuration for unsafe or inconsistent values.
func (cfg *Config) Validate() error {
	if cfg.MasterKey == "" {
		return fmt.Errorf("master key is required")
	}
	if cfg.Source.Kind == "" {
		return fmt.Errorf("source provider kind is required")
	}
	if cfg.Destination.Kind == "" {
		return fmt.Errorf("destination provider kind is required")
	}
	if cfg.Source.Endpoint == "" && cfg.Source.Kind != "aws" {
		return fmt.Errorf("source endpoint is required for non-AWS providers")
	}
	if cfg.Destination.Endpoint == "" && cfg.Destination.Kind != "aws" {
		return fmt.Errorf("destination endpoint is required for non-AWS providers")
	}

	for _, p := range []struct {
		name string
		cfg  Provider
	}{
		{"source", cfg.Source},
		{"destination", cfg.Destination},
	} {
		if p.cfg.Endpoint == "" {
			continue
		}
		if err := validateEndpoint(p.name, p.cfg.Endpoint, cfg.AllowPrivateEndpoints); err != nil {
			return err
		}
	}

	return nil
}

func validateEndpoint(role, endpoint string, allowPrivate bool) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("invalid endpoint for %s: %w", role, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("endpoint scheme for %s must be http or https: %s", role, endpoint)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("endpoint host for %s is empty: %s", role, endpoint)
	}
	if allowPrivate {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil {
		if isRestrictedIP(ip) {
			return fmt.Errorf("endpoint for %s uses a private/loopback address and allow_private_endpoints is false: %s", role, endpoint)
		}
	}
	// For hostnames, no generic private-network detection without DNS; leave to operators.
	return nil
}

func isRestrictedIP(ip net.IP) bool {
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast()
}

// ValidateFunc is a package-level alias for tests.
func ValidateFunc(cfg *Config) error {
	return cfg.Validate()
}
