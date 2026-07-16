package amitest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"time"
)

// LocalhostTLS generates a fresh self-signed certificate for loopback
// TLS scenarios and returns the matching pair: a server configuration
// for [Config].TLS and a client configuration trusting exactly that
// certificate. The certificate covers localhost, 127.0.0.1, and ::1
// for 24 hours; nothing is persisted, so no fixture with an embedded
// expiry date can rot in a repository. It panics when generation
// fails, because a test host without working crypto is broken, not a
// condition to handle.
func LocalhostTLS() (server, client *tls.Config) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic("amitest: generate key: " + err.Error())
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		panic("amitest: generate serial: " + err.Error())
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "amitest"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic("amitest: create certificate: " + err.Error())
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		panic("amitest: parse certificate: " + err.Error())
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	server = &tls.Config{Certificates: []tls.Certificate{{
		Certificate: [][]byte{der},
		PrivateKey:  key,
		Leaf:        leaf,
	}}}
	client = &tls.Config{RootCAs: pool}
	return server, client
}
