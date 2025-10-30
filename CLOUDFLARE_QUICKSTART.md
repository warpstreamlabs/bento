# Cloudflare Containers - Quick Start

Deploy Bento to Cloudflare Containers in 5 minutes.

## Prerequisites

- Cloudflare account with Workers Paid plan ($5/month)
- Docker installed and running
- Node.js and npm installed

## Quick Deploy

### 1. Install Wrangler

```bash
npm install -g wrangler
```

### 2. Login to Cloudflare

```bash
wrangler login
```

### 3. Update Configuration

Edit `wrangler.toml` and replace placeholders:

```toml
name = "bento-stream-processor"  # Change to your preferred name
```

### 4. Deploy

```bash
# Using the deployment script (recommended)
./scripts/deploy-cloudflare.sh

# Or manually
wrangler deploy
```

### 5. Wait for Provisioning

**Important**: Wait 2-5 minutes after first deployment for containers to be provisioned.

### 6. Test

```bash
# Get your Worker URL from the deployment output, then:
curl https://bento-stream-processor.your-subdomain.workers.dev/ping
```

Expected response: `200 OK`

## What's Next?

### Configure Cloudflare R2 (Optional)

1. Create R2 buckets:
   ```bash
   wrangler r2 bucket create bento-input
   wrangler r2 bucket create bento-output
   ```

2. Create R2 API token from Cloudflare Dashboard > R2 > Manage R2 API Tokens

3. Update `.env` with your credentials:
   ```bash
   cp .env.example .env
   # Edit .env with your R2 credentials
   ```

4. Update `wrangler.toml` R2 bindings:
   ```toml
   [[r2_buckets]]
   binding = "BENTO_STORAGE"
   bucket_name = "bento-input"
   ```

### Customize Bento Configuration

Edit `config/cloudflare.yaml` to configure your stream processing pipeline:

```yaml
input:
  http_server:
    address: 0.0.0.0:4195
    path: /process

pipeline:
  processors:
    - mapping: |
        root = this
        root.processed_at = now()

output:
  stdout: {}
```

### Monitor Your Deployment

```bash
# View logs
wrangler tail

# List deployments
wrangler deployments list

# Check metrics (access /metrics endpoint)
curl https://your-worker.workers.dev/metrics
```

## Common Issues

### "Container not ready"

Wait a few minutes after deployment. Containers take time to provision on first deployment.

### Docker errors

Make sure Docker is running:
```bash
docker ps
```

### Deployment fails

Check your Cloudflare account has the Workers Paid plan:
```bash
wrangler whoami
```

## Full Documentation

See [CLOUDFLARE_DEPLOYMENT.md](CLOUDFLARE_DEPLOYMENT.md) for complete documentation.

## Examples

### Stream Processing

```yaml
# config/cloudflare.yaml
input:
  http_server:
    address: 0.0.0.0:4195

pipeline:
  processors:
    - mapping: 'root.processed = true'

output:
  aws_s3:  # Uses Cloudflare R2
    bucket: ${R2_BUCKET_NAME}
    endpoint: ${R2_ENDPOINT}
```

### Data Transformation

```yaml
input:
  aws_s3:  # Read from R2
    bucket: raw-data
    endpoint: ${R2_ENDPOINT}

pipeline:
  processors:
    - mapping: |
        root = this.parse_json()
        root.transformed = true

output:
  aws_s3:  # Write to R2
    bucket: processed-data
    endpoint: ${R2_ENDPOINT}
```

## Support

- 📖 [Full Documentation](CLOUDFLARE_DEPLOYMENT.md)
- 💬 [Discord](https://console.warpstream.com/socials/discord)
- 💼 [Slack](https://console.warpstream.com/socials/slack)
- 🐛 [GitHub Issues](https://github.com/warpstreamlabs/bento/issues)
