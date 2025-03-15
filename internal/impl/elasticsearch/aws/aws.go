package aws

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/olivere/elastic/v7"

	baws "github.com/warpstreamlabs/bento/internal/impl/aws"
	"github.com/warpstreamlabs/bento/internal/impl/elasticsearch"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	elasticsearch.AWSOptFn = func(conf *service.ParsedConfig, client *http.Client) ([]elastic.ClientOptionFunc, error) {
		if enabled, _ := conf.FieldBool(elasticsearch.ESOFieldAWSEnabled); !enabled {
			return nil, nil
		}

		tsess, err := baws.GetSession(context.TODO(), conf)
		if err != nil {
			return nil, err
		}

		region := tsess.Region
		if region == "" {
			return nil, errors.New("unable to detect target AWS region, if you encounter this error please report it via: https://github.com/warpstreamlabs/bento/issues/new")
		}

		creds, err := tsess.Credentials.Retrieve(context.TODO())
		if err != nil {
			return nil, err
		}

		signerTransport := &awsSignerTransport{
			RoundTripper: client.Transport,
			signer:       v4.NewSigner(),
			creds:        creds,
			region:       tsess.Region,
			service:      "es",
		}
		client.Transport = signerTransport

		return []elastic.ClientOptionFunc{elastic.SetHttpClient(client)}, nil
	}
}

// The ElasticSearch service requires requests be properly signed. Since the ElasticSearch library is quite outdated, it
// relies on aws-sdk-go v1. Instead, we sign our own requests using the awsSignerTransport transport wrapper.

// awsSignerTransport signs HTTP requests to AWS using passed in credentials.
type awsSignerTransport struct {
	http.RoundTripper

	signer *v4.Signer

	creds   aws.Credentials
	region  string
	service string
}

func (t *awsSignerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	hash, err := computePayloadHash(req)
	if err != nil {
		return nil, fmt.Errorf("failed to compute SHA256 for request payload: %w", err)
	}

	err = t.signer.SignHTTP(context.TODO(), t.creds, req, hash, t.service, t.region, time.Now())
	if err != nil {
		return nil, err
	}
	return t.RoundTripper.RoundTrip(req)
}

func computePayloadHash(req *http.Request) (string, error) {
	// If the body is empty, use the empty string SHA.
	// See https://github.com/aws/aws-sdk-go-v2/blob/a8a6a95eeb38a9d95edb2c3eaec8ce7284c6202a/aws/signer/v4/v4.go#L249-L260
	if req.Body == nil {
		return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", nil
	}

	// Consume all bytes from the request's body Reader
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return "", err
	}
	if err := req.Body.Close(); err != nil {
		return "", err
	}

	if len(body) == 0 {
		return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", nil
	}

	h := sha256.New()
	h.Write(body)
	payloadHash := hex.EncodeToString(h.Sum(nil))

	// Now we have to set the body Reader back
	req.Body = io.NopCloser(bytes.NewReader(body))

	return payloadHash, nil
}
