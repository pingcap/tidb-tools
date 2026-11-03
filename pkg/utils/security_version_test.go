// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"crypto/tls"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTLSMinimumVersion(t *testing.T) {
	ca, err := os.ReadFile("tls_test/ca.pem")
	require.NoError(t, err)
	cert, err := os.ReadFile("tls_test/client1.pem")
	require.NoError(t, err)
	key, err := os.ReadFile("tls_test/client1.key")
	require.NoError(t, err)
	serverCert, err := tls.LoadX509KeyPair("tls_test/server.pem", "tls_test/server.key")
	require.NoError(t, err)

	for _, constructor := range []struct {
		name  string
		build func() (*tls.Config, error)
	}{
		{"files", func() (*tls.Config, error) {
			return ToTLSConfigWithVerify("tls_test/ca.pem", "tls_test/client1.pem", "tls_test/client1.key", nil)
		}},
		{"bytes", func() (*tls.Config, error) {
			return ToTLSConfigWithVerifyByRawbytes(ca, cert, key, nil)
		}},
	} {
		t.Run(constructor.name, func(t *testing.T) {
			for _, version := range []uint16{tls.VersionTLS10, tls.VersionTLS11, tls.VersionTLS12, tls.VersionTLS13} {
				t.Run(tls.VersionName(version), func(t *testing.T) {
					server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusOK)
					}))
					server.Config.ErrorLog = log.New(io.Discard, "", 0)
					server.TLS = &tls.Config{
						Certificates: []tls.Certificate{serverCert},
						MinVersion:   version,
						MaxVersion:   version,
					}
					server.StartTLS()
					defer server.Close()

					cfg, err := constructor.build()
					require.NoError(t, err)
					transport := &http.Transport{TLSClientConfig: cfg}
					defer transport.CloseIdleConnections()
					client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
					resp, err := client.Get(server.URL)
					if version < tls.VersionTLS12 {
						if resp != nil {
							resp.Body.Close()
						}
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					defer resp.Body.Close()
					require.Equal(t, http.StatusOK, resp.StatusCode)
					require.Equal(t, version, resp.TLS.Version)
				})
			}
		})
	}
}
