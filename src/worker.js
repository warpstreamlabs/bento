/**
 * Cloudflare Worker for Bento Container
 * This worker manages the lifecycle of Bento stream processor containers
 */

export class BentoContainer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request) {
    // Get or create container instance
    const container = await this.state.getContainer();

    if (!container) {
      // Container not ready yet
      return new Response('Container starting...', {
        status: 503,
        headers: { 'Retry-After': '5' }
      });
    }

    // Proxy request to Bento container
    // Bento's default HTTP server runs on port 4195
    const url = new URL(request.url);
    const containerUrl = `http://localhost:4195${url.pathname}${url.search}`;

    try {
      const response = await container.fetch(containerUrl, {
        method: request.method,
        headers: request.headers,
        body: request.body,
      });

      return response;
    } catch (error) {
      return new Response(`Error connecting to Bento: ${error.message}`, {
        status: 500
      });
    }
  }
}

// Main worker export for routing
export default {
  async fetch(request, env) {
    // Extract Durable Object ID (you can customize this logic)
    // For now, we use a single instance, but you could shard by region, user, etc.
    const id = env.BENTO_CONTAINER.idFromName('default');
    const stub = env.BENTO_CONTAINER.get(id);

    return stub.fetch(request);
  },
};
