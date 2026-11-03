// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const validDMResponse = `{"result":true,"cfgs":["source-id = \"test\"\n[from]\nhost = \"127.0.0.1\"\n[to]\nhost = \"127.0.0.1\"\n"]}`

func TestDMRejectsRemotePlaintext(t *testing.T) {
	_, err := getDMTaskCfg("http://192.0.2.1:8261", "test")
	require.ErrorContains(t, err, "requires HTTPS")
}

func TestDMRejectsInvalidResponses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"status", http.StatusForbidden, validDMResponse, "HTTP 403"},
		{"empty", http.StatusOK, `{"result":true,"cfgs":[]}`, "no subtask"},
		{"invalid", http.StatusOK, "not JSON", "invalid character"},
		{"large", http.StatusOK, "", "size limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				if tc.name == "large" {
					block := make([]byte, 1<<20)
					for i := 0; i < 17; i++ {
						_, _ = w.Write(block)
					}
				} else {
					_, _ = io.WriteString(w, tc.body)
				}
			}))
			defer server.Close()
			_, err := getDMTaskCfg(server.URL, "test")
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestDMRejectsRedirects(t *testing.T) {
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("redirect destination must not receive a request")
	}))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusFound)
	}))
	defer server.Close()
	_, err := getDMTaskCfg(server.URL, "test")
	require.ErrorContains(t, err, "redirects are not allowed")
}

func TestDMCustomCAAndClientCertificate(t *testing.T) {
	ca, err := os.ReadFile("../../pkg/utils/tls_test/ca.pem")
	require.NoError(t, err)
	cert, err := tls.LoadX509KeyPair("../../pkg/utils/tls_test/server.pem", "../../pkg/utils/tls_test/server.key")
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(ca))
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.TLS.PeerCertificates)
		_, _ = io.WriteString(w, validDMResponse)
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	server.StartTLS()
	defer server.Close()
	security := &Security{CABytes: string(ca), CertPath: "../../pkg/utils/tls_test/client1.pem", KeyPath: "../../pkg/utils/tls_test/client1.key"}
	cfgs, err := getDMTaskCfgWithSecurity(server.URL, "test", security, false)
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	_, err = getDMTaskCfgWithSecurity(server.URL, "test", &Security{CABytes: string(ca)}, false)
	require.Error(t, err, "missing client certificate must be rejected")
	_, err = getDMTaskCfg(server.URL, "test")
	require.Error(t, err, "an untrusted server must be rejected")
}
