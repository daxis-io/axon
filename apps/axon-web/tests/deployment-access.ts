import type { BrowserContext } from '@playwright/test';

export async function authorizeDeploymentRequests(
  context: BrowserContext,
  baseURL: string,
  bypass: string,
): Promise<void> {
  const origin = new URL(baseURL).origin;
  await context.route(
    (url) => url.origin === origin,
    async (route) => {
      const response = await route.fetch({
        headers: { ...route.request().headers(), 'x-vercel-protection-bypass': bypass },
        maxRedirects: 0,
      });
      await route.fulfill({ response });
    },
  );
}
