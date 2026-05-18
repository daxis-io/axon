import type { ExecutionTarget, SavedQuery } from './types.ts';
import { getMetadataRecords, replaceMetadataRecords } from './metadata.ts';

const LEGACY_KEY = 'axon-editor.saved.v1';

export async function loadSaved(): Promise<SavedQuery[]> {
  const indexed = await getMetadataRecords<SavedQuery>('saved');
  if (indexed.length > 0) {
    removeLegacySaved();
    return sortSaved(indexed);
  }

  const legacy = loadLegacySaved();
  if (legacy.length > 0) {
    const sorted = sortSaved(legacy);
    await replaceMetadataRecords('saved', sorted);
    removeLegacySaved();
    return sorted;
  }

  return [];
}

type NewSaved = { name: string; sql: string; target: ExecutionTarget; owner?: string };

export async function saveQuery(input: NewSaved): Promise<SavedQuery> {
  const now = new Date();
  const entry: SavedQuery = {
    id: `s_${now.getTime()}_${Math.random().toString(36).slice(2, 6)}`,
    name: input.name.trim() || 'Untitled query',
    owner: input.owner ?? 'you',
    edited: relTime(now),
    target: input.target,
    sql: input.sql,
  };

  try {
    const all = [entry, ...(await loadSaved()).filter((s) => s.name !== entry.name)];
    await writeSaved(all);
    removeLegacySaved();
  } catch {
    // IndexedDB may be unavailable. Return the entry so the current UI can still update.
  }

  return entry;
}

export async function deleteSaved(id: string): Promise<SavedQuery[]> {
  const remaining = (await loadSaved()).filter((s) => s.id !== id);
  await writeSaved(remaining);
  return remaining;
}

function loadLegacySaved(): SavedQuery[] {
  try {
    const raw = localStorage.getItem(LEGACY_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as SavedQuery[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function removeLegacySaved(): void {
  try {
    localStorage.removeItem(LEGACY_KEY);
  } catch {
    // localStorage may be disabled.
  }
}

function writeSaved(entries: SavedQuery[]): Promise<void> {
  return replaceMetadataRecords('saved', entries);
}

function sortSaved(entries: SavedQuery[]): SavedQuery[] {
  return [...entries].sort((a, b) => b.edited.localeCompare(a.edited));
}

function relTime(date: Date): string {
  const today = new Date();
  if (
    date.getFullYear() === today.getFullYear() &&
    date.getMonth() === today.getMonth() &&
    date.getDate() === today.getDate()
  ) {
    return date.toTimeString().slice(0, 5);
  }
  const diffDays = Math.floor((today.getTime() - date.getTime()) / 86_400_000);
  if (diffDays === 1) return 'yesterday';
  if (diffDays < 7) return `${diffDays}d`;
  if (diffDays < 30) return `${Math.floor(diffDays / 7)}w`;
  return date.toISOString().slice(0, 10);
}
