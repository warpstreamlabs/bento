package aws

import (
	// Bring in the internal plugin definitions.
	_ "github.com/warpstreamlabs/bento/internal/impl/aws"
	_ "github.com/warpstreamlabs/bento/internal/impl/elasticsearch/aws"
	_ "github.com/warpstreamlabs/bento/internal/impl/kafka/aws"
	_ "github.com/warpstreamlabs/bento/internal/impl/opensearch/aws"
)
