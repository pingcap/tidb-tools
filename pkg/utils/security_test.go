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
	//"bufio"
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
	"time"

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
	c.Log(caPath)
	serverTLS, err := ToTLSConfig(caPath, certPath, keyPath, []string{"client1"})
	//serverTLS, err := ToTLSConfig(caPath, certPath, keyPath, nil)
	c.Assert(err, IsNil)

	caPath1, certPath1, keyPath1 := getTestCertFile(dir, "client1")
	clientTLS1, err := ToTLSConfig(caPath1, certPath1, keyPath1, nil)
	c.Assert(err, IsNil)
	//tlsClient1, err := NewTLS(caPath1, certPath1, keyPath1, "127.0.0.1", nil)
	//c.Assert(err, IsNil)

	caPath2, certPath2, keyPath2 := getTestCertFile(dir, "client2")
	//tlsClient2, err := NewTLS(caPath2, certPath2, keyPath2, "127.0.0.1", nil)
	//c.Assert(err, IsNil)

	

	//caPath, certPath, keyPath = getTestCertFile(dir, "client2")
	clientTLS2, err := ToTLSConfig(caPath2, certPath2, keyPath2, nil)
	//c.Assert(err, IsNil)
	c.Log("run_server")
	err = AddClientCAs(clientTLS1, []string{caPath1, certPath1, certPath2, certPath})
	c.Assert(err, IsNil)
	err = AddClientCAs(clientTLS2, []string{caPath1, certPath1, certPath2, certPath})
	c.Assert(err, IsNil)

	err = AddClientCAs(serverTLS, []string{caPath1, certPath1, certPath2, certPath})
	c.Assert(err, IsNil)

	err = AddRootCAs(clientTLS1, []string{caPath1, certPath1, certPath2, certPath})
	c.Assert(err, IsNil)
	err = AddRootCAs(clientTLS2, []string{caPath1, certPath1, certPath2, certPath})
	c.Assert(err, IsNil)

	err = AddRootCAs(serverTLS, []string{caPath1, certPath1, certPath2, certPath})
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	server := runServer(ctx, *serverTLS, 123, c)
	defer func() {
		cancel()
		server.Close()
	}()

	//err = runClient(*clientTLS1, 123, c)
	//c.Assert(err, IsNil)
	//err = runClient(*clientTLS2, 123, c)
	//c.Assert(err, IsNil)
	resp, err := ClientWithTLS(clientTLS1).Get("https://127.0.0.1:123")
	c.Log(resp)
	c.Assert(err, IsNil)

	resp, err = ClientWithTLS(clientTLS2).Get("https://127.0.0.1:123/")
	c.Log(resp)
	c.Assert(err, ErrorMatches, ".*tls: bad certificate")

	time.Sleep(time.Second)
	//c.Assert(err, NotNil)

	cancel()
}

func runClient(tlsCfg tls.Config, port int, c *C) error {
	tlsCfg.InsecureSkipVerify = true
	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), &tlsCfg)
	if err != nil {
		c.Log("134", err)
		return err
	}
	defer conn.Close()

	_, err = conn.Write([]byte("hello\n"))
	if err != nil {
		c.Log("141", err)
		return err
	}

	buf := make([]byte, 100)
	n, err := conn.Read(buf)
	if err != nil {
		c.Log("148", err)
		return err
	}

	c.Log(string(buf[:n]))
	return nil
}

func handler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("This an example server.\n"))
}

func runServer(ctx context.Context, tlsCfg tls.Config, port int, c *C) *http.Server {
	http.HandleFunc("/", handler)
	server := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: nil}

	conn, err := net.Listen("tcp", server.Addr)
	if err != nil {
	  c.Assert(err, IsNil)
	}
	
	tlsListener := tls.NewListener(conn, &tlsCfg)
	go server.Serve(tlsListener)
	return server
	/*
	//tlsCfg.InsecureSkipVerify = true
	ln, err := tls.Listen("tcp", fmt.Sprintf(":%d", port), &tlsCfg)
	c.Assert(err, IsNil)

	go func() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		for {
			conn, err := ln.Accept()
			c.Assert(err, IsNil)
			c.Log("accept")
			handleConnection(conn, c)
		}
	}()
	return ln
	*/
}

/*
func handleConnection(conn net.Conn, c *C) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		msg, err := r.ReadString('\n')
		if err == io.EOF {
			break
		}
		c.Assert(err, IsNil)

		c.Log(msg)

		_, err = conn.Write([]byte("world\n"))
		c.Assert(err, IsNil)
	}
}
*/

func getTestCertFile(dir, role string) (string, string, string) {
	return path.Join(dir, "ca.pem"), path.Join(dir, fmt.Sprintf("%s.pem", role)), path.Join(dir, fmt.Sprintf("%s.key", role))
}
