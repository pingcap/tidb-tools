package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/juju/errors"
)

// ToTLSConfig generates tls's config.
func ToTLSConfig(SSLCA, SSLCert, SSLKey string) (*tls.Config, error) {
	var tlsConfig *tls.Config
	if len(SSLCA) != 0 {
		var certificates = make([]tls.Certificate, 0)
		if len(SSLCert) != 0 && len(SSLKey) != 0 {
			// Load the client certificates from disk
			certificate, err := tls.LoadX509KeyPair(SSLCert, SSLKey)
			if err != nil {
				return nil, errors.Errorf("could not load client key pair: %s", err)
			}
			certificates = append(certificates, certificate)
		}

		// Create a certificate pool from the certificate authority
		certPool := x509.NewCertPool()
		ca, err := ioutil.ReadFile(SSLCA)
		if err != nil {
			return nil, errors.Errorf("could not read ca certificate: %s", err)
		}

		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(ca) {
			return nil, errors.New("failed to append ca certs")
		}

		tlsConfig = &tls.Config{
			Certificates: certificates,
			RootCAs:      certPool,
		}
	}

	return tlsConfig, nil
}
