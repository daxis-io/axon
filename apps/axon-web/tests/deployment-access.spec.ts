import { once } from 'node:events';
import { createServer, type Server } from 'node:http';
import { expect, test } from '@playwright/test';
import { authorizeDeploymentRequests } from './deployment-access.ts';

function origin(server: Server): string {
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('expected a TCP server');
  return `http://127.0.0.1:${address.port}`;
}

test('keeps a deployment bypass out of cross-origin redirects', async ({ page, context }) => {
  let sourceHeader: string | string[] | undefined;
  let destinationHeader: string | string[] | undefined;
  const destination = createServer((request, response) => {
    destinationHeader = request.headers['x-vercel-protection-bypass'];
    response.end('redirect destination');
  }).listen(0, '127.0.0.1');
  await once(destination, 'listening');
  const source = createServer((request, response) => {
    sourceHeader = request.headers['x-vercel-protection-bypass'];
    response.writeHead(302, { location: origin(destination) });
    response.end();
  }).listen(0, '127.0.0.1');
  await once(source, 'listening');

  try {
    await authorizeDeploymentRequests(context, origin(source), 'test-deployment-bypass');
    await page.goto(origin(source));
    expect(sourceHeader).toBe('test-deployment-bypass');
    expect(destinationHeader).toBeUndefined();
  } finally {
    await page.close();
    await Promise.all(
      [source, destination].map(
        (server) =>
          new Promise<void>((resolve, reject) => {
            server.close((error) => (error ? reject(error) : resolve()));
            server.closeAllConnections();
          }),
      ),
    );
  }
});
