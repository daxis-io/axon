// Minimal vanilla router. Uses the History API directly — no dependency.
// Routes are paths; navigation pushes state and notifies subscribers.

import { useEffect, useState } from 'react';

export type Route = '/' | '/connect' | '/catalogs';

const KNOWN: Route[] = ['/', '/connect', '/catalogs'];

export function resolveRoute(pathname: string): Route {
  for (const r of KNOWN) {
    if (r !== '/' && (pathname === r || pathname.startsWith(r + '/'))) return r;
  }
  return '/';
}

export function getCurrentRoute(): Route {
  return resolveRoute(window.location.pathname);
}

export function navigate(next: Route): void {
  if (window.location.pathname === next) return;
  window.history.pushState({}, '', next);
  window.dispatchEvent(new PopStateEvent('popstate'));
}

export function useRoute(): Route {
  const [route, setRoute] = useState<Route>(() => getCurrentRoute());
  useEffect(() => {
    const onPop = () => setRoute(getCurrentRoute());
    window.addEventListener('popstate', onPop);
    return () => window.removeEventListener('popstate', onPop);
  }, []);
  return route;
}
