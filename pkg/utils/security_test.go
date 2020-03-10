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

package common

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
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

	tls, err := NewTLS("", "", "", u.Host)
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
	_, err := NewTLS(caPath, "", "", "localhost")
	c.Assert(err, ErrorMatches, "could not read ca certificate:.*")

	err = ioutil.WriteFile(caPath, []byte("invalid ca content"), 0644)
	c.Assert(err, IsNil)
	_, err = NewTLS(caPath, "", "", "localhost")
	c.Assert(err, ErrorMatches, "failed to append ca certs")

	certPath := filepath.Join(tempDir, "test.pem")
	keyPath := filepath.Join(tempDir, "test.key")
	_, err = NewTLS(caPath, certPath, keyPath, "localhost")
	c.Assert(err, ErrorMatches, "could not load client key pair: open.*")

	err = ioutil.WriteFile(certPath, []byte("invalid cert content"), 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(keyPath, []byte("invalid key content"), 0600)
	c.Assert(err, IsNil)
	_, err = NewTLS(caPath, certPath, keyPath, "localhost")
	c.Assert(err, ErrorMatches, "could not load client key pair: tls.*")
}
