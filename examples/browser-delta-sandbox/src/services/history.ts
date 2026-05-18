import type { HistoryEntry } from './types.ts';
import { clearMetadataRecords, getMetadataRecords, replaceMetadataRecords } from './metadata.ts';

const LEGACY_KEY = 'axon-editor.history.v1';
const MAX_ENTRIES = 100;

export async function loadHistory(): Promise<HistoryEntry[]> {
  const indexed = await getMetadataRecords<HistoryEntry>('history');
  if (indexed.length > 0) {
    removeLegacyHistory();
    return sortedHistory(indexed).slice(0, MAX_ENTRIES);
  }

  const legacy = loadLegacyHistory();
  if (legacy.length > 0) {
    const sorted = sortedHistory(legacy).slice(0, MAX_ENTRIES);
    await replaceMetadataRecords('history', sorted);
    removeLegacyHistory();
    return sorted;
  }

  return [];
}

export async function appendHistory(
  entry: Omit<HistoryEntry, 'id' | 'time' | 'iso'>,
): Promise<HistoryEntry> {
  const now = new Date();
  const full: HistoryEntry = {
    id: `h_${now.getTime()}_${Math.random().toString(36).slice(2, 6)}`,
    time: now.toTimeString().slice(0, 8),
    iso: now.toISOString(),
    ...entry,
  };

  try {
    const next = [full, ...(await loadHistory()).filter((item) => item.id !== full.id)].slice(
      0,
      MAX_ENTRIES,
    );
    await replaceMetadataRecords('history', next);
    removeLegacyHistory();
  } catch {
    // IndexedDB may be unavailable in private browsing modes. Keep the in-memory UI update.
  }

  return full;
}

export async function clearHistory(): Promise<void> {
  try {
    await clearMetadataRecords('history');
    removeLegacyHistory();
  } catch {
    // ignore
  }
}

function loadLegacyHistory(): HistoryEntry[] {
  try {
    const raw = localStorage.getItem(LEGACY_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as HistoryEntry[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function removeLegacyHistory(): void {
  try {
    localStorage.removeItem(LEGACY_KEY);
  } catch {
    // localStorage may be disabled.
  }
}

function sortedHistory(entries: HistoryEntry[]): HistoryEntry[] {
  return [...entries].sort((a, b) => new Date(b.iso).getTime() - new Date(a.iso).getTime());
}
