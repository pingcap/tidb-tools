// Copyright 2020 PingCAP, Inc.
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

package utils

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"

	. "github.com/pingcap/check"
)

type securitySuite struct{}

var _ = Suite(&securitySuite{})

func respondPathHandler(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, `{"path":"`)
	io.WriteString(w, req.URL.Path)
	io.WriteString(w, `"}`)
}

func (s *securitySuite) TestGetJSONInsecure(c *C) {
	mockServer := httptest.NewServer(http.HandlerFunc(respondPathHandler))
	defer mockServer.Close()

	u, err := url.Parse(mockServer.URL)
	c.Assert(err, IsNil)

	tls, err := NewTLS("", "", "", u.Host, nil)
	c.Assert(err, IsNil)

	var result struct{ Path string }
	err = tls.GetJSON("/aaa", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Path, Equals, "/aaa")
	err = tls.GetJSON("/bbbb", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Path, Equals, "/bbbb")
}

func (s *securitySuite) TestGetJSONSecure(c *C) {
	mockServer := httptest.NewTLSServer(http.HandlerFunc(respondPathHandler))
	defer mockServer.Close()

	tls := NewTLSFromMockServer(mockServer)

	var result struct{ Path string }
	err := tls.GetJSON("/ccc", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Path, Equals, "/ccc")
	err = tls.GetJSON("/dddd", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Path, Equals, "/dddd")
}

func (s *securitySuite) TestInvalidTLS(c *C) {
	tempDir := c.MkDir()

	caPath := filepath.Join(tempDir, "ca.pem")
	_, err := NewTLS(caPath, "", "", "localhost", nil)
	c.Assert(err, ErrorMatches, "could not read ca certificate:.*")

	err = ioutil.WriteFile(caPath, []byte("invalid ca content"), 0644)
	c.Assert(err, IsNil)
	_, err = NewTLS(caPath, "", "", "localhost", nil)
	c.Assert(err, ErrorMatches, "failed to append ca certs")

	certPath := filepath.Join(tempDir, "test.pem")
	keyPath := filepath.Join(tempDir, "test.key")
	_, err = NewTLS(caPath, certPath, keyPath, "localhost", nil)
	c.Assert(err, ErrorMatches, "could not load client key pair: open.*")

	err = ioutil.WriteFile(certPath, []byte("invalid cert content"), 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(keyPath, []byte("invalid key content"), 0600)
	c.Assert(err, IsNil)
	_, err = NewTLS(caPath, certPath, keyPath, "localhost", nil)
	c.Assert(err, ErrorMatches, "could not load client key pair: tls.*")
}

func (s *securitySuite) TestCheckCN(c *C) {
	dir, err := os.Getwd()
	c.Assert(err, IsNil)

	dir = path.Join(dir, "tls_test")
	caPath, certPath, keyPath := getTestCertFile(dir, "server")
	// only allow client1 to visit
	serverTLS, err := ToTLSConfigWithVerify(caPath, certPath, keyPath, []string{"client1"})
	c.Assert(err, IsNil)

	caPath1, certPath1, keyPath1 := getTestCertFile(dir, "client1")
	caData, certData, keyData := loadTLSContent(c, caPath1, certPath1, keyPath1)
	clientTLS1, err := ToTLSConfigWithVerifyByRawbytes(caData, certData, keyData, []string{})
	c.Assert(err, IsNil)

	caPath2, certPath2, keyPath2 := getTestCertFile(dir, "client2")
	clientTLS2, err := ToTLSConfigWithVerify(caPath2, certPath2, keyPath2, nil)
	c.Assert(err, IsNil)

	port := 9292
	url := fmt.Sprintf("https://127.0.0.1:%d", port)
	ctx, cancel := context.WithCancel(context.Background())
	server := runServer(ctx, serverTLS, port, c)
	defer func() {
		cancel()
		server.Close()
	}()

	resp, err := ClientWithTLS(clientTLS1).Get(url)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(body), Equals, "This an example server")

	// client2 can't visit server
	_, err = ClientWithTLS(clientTLS2).Get(url)
	c.Assert(err, ErrorMatches, ".*tls: bad certificate")
}

func handler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("This an example server"))
}

func runServer(ctx context.Context, tlsCfg *tls.Config, port int, c *C) *http.Server {
	http.HandleFunc("/", handler)
	server := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: nil}

	conn, err := net.Listen("tcp", server.Addr)
	if err != nil {
		c.Assert(err, IsNil)
	}

	tlsListener := tls.NewListener(conn, tlsCfg)
	go server.Serve(tlsListener)
	return server
}

func getTestCertFile(dir, role string) (string, string, string) {
	return path.Join(dir, "ca.pem"), path.Join(dir, fmt.Sprintf("%s.pem", role)), path.Join(dir, fmt.Sprintf("%s.key", role))
}

func loadTLSContent(c *C, caPath, certPath, keyPath string) (caData, certData, keyData []byte) {
	// NOTE we make sure the file exists,so we don't need to check the error
	var err error
	caData, err = ioutil.ReadFile(caPath)
	c.Assert(err, IsNil)

	certData, err = ioutil.ReadFile(certPath)
	c.Assert(err, IsNil)

	keyData, err = ioutil.ReadFile(keyPath)
	c.Assert(err, IsNil)
	return
}
