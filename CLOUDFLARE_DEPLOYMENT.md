# Deploying Bento to Cloudflare Containers

This guide explains how to deploy Bento stream processor to Cloudflare Containers, which are now available in public beta (as of June 2025).

## Overview

Bento can be deployed to Cloudflare's global container platform, providing:

- **Global deployment**: Deploy to "Region: Earth" - your containers run close to your users worldwide
- **S3-compatible storage**: Use Cloudflare R2 for input/output storage
- **Serverless scaling**: Automatic scale-to-zero with fast cold starts (~2-3 seconds)
- **Simple deployment**: Use `wrangler deploy` just like Cloudflare Workers

## Prerequisites

1. **Cloudflare Account**: A Cloudflare account with a paid Workers plan ($5/month minimum)
2. **Wrangler CLI**: Install the latest version of Wrangler
   ```bash
   npm install -g wrangler
   ```
3. **Docker**: Docker must be running locally for building container images
4. **Authentication**: Log in to Cloudflare
   ```bash
   wrangler login
   ```

## Architecture

Bento on Cloudflare Containers uses:

- **Durable Objects**: Manages container lifecycle
- **Workers**: Routes requests to container instances
- **Container Registry**: Stores your Bento container image
- **R2 Storage** (optional): S3-compatible object storage for data
- **KV** (optional): Key-value storage for metadata

## Configuration Files

### 1. wrangler.toml

The main configuration file for Cloudflare deployment:

```toml
name = "bento-stream-processor"
main = "src/worker.js"
compatibility_date = "2025-01-15"

[[durable_objects.bindings]]
name = "BENTO_CONTAINER"
class_name = "BentoContainer"
script_name = "bento-stream-processor"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["BentoContainer"]

[containers.BENTO_CONTAINER]
image = "./resources/docker/Dockerfile"
instance_type = "standard-1"
max_instances = 10
```

### 2. Bento Configuration (config/cloudflare.yaml)

Configure Bento to use Cloudflare R2:

```yaml
input:
  aws_s3:
    bucket: ${R2_BUCKET_NAME}
    endpoint: ${R2_ENDPOINT}
    credentials:
      id: ${R2_ACCESS_KEY_ID}
      secret: ${R2_SECRET_ACCESS_KEY}
    force_path_style_urls: true
```

## Step-by-Step Deployment

### Step 1: Set up Cloudflare R2 (Optional)

If you want to use Cloudflare R2 for storage:

```bash
# Create R2 buckets
wrangler r2 bucket create bento-input
wrangler r2 bucket create bento-output

# Create R2 API token
# Go to Cloudflare Dashboard > R2 > Manage R2 API Tokens
# Copy the Access Key ID and Secret Access Key
```

### Step 2: Configure Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` and fill in your values:

```bash
R2_ACCOUNT_ID=your-account-id
R2_ENDPOINT=https://your-account-id.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=your-r2-access-key-id
R2_SECRET_ACCESS_KEY=your-r2-secret-access-key
R2_BUCKET_NAME=bento-input
R2_OUTPUT_BUCKET_NAME=bento-output
```

### Step 3: Update wrangler.toml with Your Account Details

Edit `wrangler.toml` and update:

1. Account ID
2. R2 bucket bindings
3. KV namespace IDs (if using KV)

### Step 4: Build and Deploy

```bash
# Make sure Docker is running
docker ps

# Deploy to Cloudflare Containers
wrangler deploy
```

The first deployment will:
1. Build your container image using Docker
2. Push the image to Cloudflare Container Registry
3. Deploy your Worker and configure Durable Objects
4. Provision container instances (this takes a few minutes)

### Step 5: Wait for Provisioning

**Important**: After the first deployment, wait several minutes for containers to be provisioned before sending requests.

### Step 6: Test Your Deployment

```bash
# Check health endpoint
curl https://bento-stream-processor.your-subdomain.workers.dev/ping

# Check readiness
curl https://bento-stream-processor.your-subdomain.workers.dev/ready

# Test stream processing (example)
curl -X POST https://bento-stream-processor.your-subdomain.workers.dev/process \\
  -H "Content-Type: application/json" \\
  -d '{"message": "Hello from Cloudflare!"}'
```

## Instance Types

Choose the right instance type for your workload:

| Instance Type | RAM    | vCPU | Disk  | Use Case                    |
|--------------|--------|------|-------|-----------------------------|
| lite         | 128 MB | 0.1  | 1 GB  | Light processing            |
| basic        | 256 MB | 0.2  | 2 GB  | Small workloads             |
| standard-1   | 1 GB   | 1    | 10 GB | General purpose (default)   |
| standard-2   | 2 GB   | 2    | 20 GB | Medium workloads            |
| standard-3   | 4 GB   | 4    | 40 GB | Heavy processing            |
| standard-4   | 8 GB   | 8    | 80 GB | Maximum capacity            |

Update in `wrangler.toml`:

```toml
[containers.BENTO_CONTAINER]
instance_type = "standard-2"  # Change as needed
```

## Using Cloudflare R2 with Bento

Bento's AWS S3 connectors are fully compatible with Cloudflare R2:

### Input from R2

```yaml
input:
  aws_s3:
    bucket: ${R2_BUCKET_NAME}
    endpoint: ${R2_ENDPOINT}
    credentials:
      id: ${R2_ACCESS_KEY_ID}
      secret: ${R2_SECRET_ACCESS_KEY}
    force_path_style_urls: true
```

### Output to R2

```yaml
output:
  aws_s3:
    bucket: ${R2_OUTPUT_BUCKET_NAME}
    path: "output/${!timestamp_unix}.json"
    endpoint: ${R2_ENDPOINT}
    credentials:
      id: ${R2_ACCESS_KEY_ID}
      secret: ${R2_SECRET_ACCESS_KEY}
    force_path_style_urls: true
```

## Monitoring

### Health Checks

Bento provides built-in health check endpoints:

- `/ping` - Liveness probe (always returns 200)
- `/ready` - Readiness probe (returns 200 when input/output connected)

### Metrics

Configure Prometheus metrics in your Bento config:

```yaml
metrics:
  prometheus:
    use_histogram_timing: false
```

Access metrics at: `https://your-worker.workers.dev/metrics`

### Logs

View logs using Wrangler:

```bash
# Tail logs in real-time
wrangler tail

# Filter logs
wrangler tail --format pretty
```

## Scaling

### Horizontal Scaling

Configure maximum instances in `wrangler.toml`:

```toml
[containers.BENTO_CONTAINER]
max_instances = 10  # Adjust based on your needs
```

Cloudflare automatically scales containers based on demand, down to zero when idle.

### Vertical Scaling

Choose a larger instance type for more CPU/memory:

```toml
[containers.BENTO_CONTAINER]
instance_type = "standard-3"  # 4 GB RAM, 4 vCPU
```

## Deployment Strategies

### Immediate Rollout (Default)

Updates all container instances immediately:

```bash
wrangler deploy
```

### Gradual Rollout

For safer deployments, use gradual rollout strategies (check Cloudflare docs for latest options).

## Cost Optimization

### Scale-to-Zero

Containers automatically scale to zero when not in use. You only pay for:
- Active container compute time
- R2 storage and requests
- Workers requests

### Right-Size Instances

Start with `lite` or `basic` instances and scale up as needed.

## Limitations (as of 2025)

- Cold starts: ~2-3 seconds
- Ephemeral disk: Containers lose data on restart (use R2 for persistence)
- Max size: 40 GB RAM, 40 vCPU per container
- Architecture: Must be linux/amd64

## Troubleshooting

### Container Not Starting

```bash
# Check deployment status
wrangler deployments list

# View container logs
wrangler tail --format pretty
```

### Connection Issues

1. Verify Docker is running: `docker ps`
2. Check image builds locally: `docker build -f resources/docker/Dockerfile .`
3. Verify R2 credentials and endpoint URLs

### Slow Cold Starts

- Consider using a smaller instance type for faster starts
- Keep containers warm with periodic health checks
- Use Cloudflare Cron Triggers to prevent scale-to-zero

## Example Use Cases

### 1. Stream Processing Pipeline

```yaml
input:
  http_server:
    address: 0.0.0.0:4195
    path: /ingest

pipeline:
  processors:
    - mapping: |
        root = this
        root.processed_at = now()

output:
  aws_s3:
    bucket: ${R2_OUTPUT_BUCKET_NAME}
    endpoint: ${R2_ENDPOINT}
```

### 2. Data Transformation

```yaml
input:
  aws_s3:
    bucket: raw-data
    endpoint: ${R2_ENDPOINT}

pipeline:
  processors:
    - mapping: |
        root.transformed = this.parse_json()

output:
  aws_s3:
    bucket: processed-data
    endpoint: ${R2_ENDPOINT}
```

## Additional Resources

- [Cloudflare Containers Documentation](https://developers.cloudflare.com/containers/)
- [Bento Documentation](https://warpstreamlabs.github.io/bento/docs/about)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)
- [Wrangler CLI Reference](https://developers.cloudflare.com/workers/wrangler/)

## Support

- GitHub Issues: [warpstreamlabs/bento](https://github.com/warpstreamlabs/bento/issues)
- Discord: [WarpStream Discord](https://console.warpstream.com/socials/discord)
- Slack: [WarpStream Slack](https://console.warpstream.com/socials/slack)
