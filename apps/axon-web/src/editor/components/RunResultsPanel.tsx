import {
  sameQueryResultPageRun,
  type QueryResultPageRun,
} from '../../services/query-pagination.ts';
import type { CommitEntry, HistoryEntry } from '../../services/types.ts';
import {
  selectRunCapabilities,
  selectRunEvents,
  selectRunLoadingMoreRows,
  selectRunMetrics,
  selectRunPlan,
  selectRunResultData,
  selectRunResultPageRun,
  selectRunState,
  useAxonClientStore,
} from '../../state/hooks.ts';
import { Results } from './Results.tsx';

type RunResultsPanelProps = {
  history: HistoryEntry[];
  serverFallbackEnabled: boolean;
  commits: CommitEntry[];
  snapshotPin: number | null;
  tableSnapshot: number | undefined;
  tableUri: string | undefined;
  protocolVersion: { reader: number; writer: number; features: string[] } | undefined;
  activeResultPageRun: QueryResultPageRun | undefined;
  onLoadMoreRows: () => void;
};

export function RunResultsPanel({
  history,
  serverFallbackEnabled,
  commits,
  snapshotPin,
  tableSnapshot,
  tableUri,
  protocolVersion,
  activeResultPageRun,
  onLoadMoreRows,
}: RunResultsPanelProps) {
  const runState = useAxonClientStore(selectRunState);
  const resultData = useAxonClientStore(selectRunResultData);
  const resultPageRun = useAxonClientStore(selectRunResultPageRun);
  const loadingMoreRows = useAxonClientStore(selectRunLoadingMoreRows);
  const metrics = useAxonClientStore(selectRunMetrics);
  const events = useAxonClientStore(selectRunEvents);
  const plan = useAxonClientStore(selectRunPlan);
  const capabilities = useAxonClientStore(selectRunCapabilities);
  const canLoadMoreRows =
    resultData?.page?.has_more === true &&
    resultPageRun !== undefined &&
    activeResultPageRun !== undefined &&
    sameQueryResultPageRun(resultPageRun, activeResultPageRun);

  return (
    <Results
      runState={runState}
      resultData={resultData}
      metrics={metrics}
      events={events}
      history={history}
      serverFallbackEnabled={serverFallbackEnabled}
      plan={plan}
      commits={commits}
      capabilities={capabilities}
      snapshotPin={snapshotPin}
      tableSnapshot={tableSnapshot}
      tableUri={tableUri}
      loadingMoreRows={loadingMoreRows}
      onLoadMoreRows={canLoadMoreRows ? onLoadMoreRows : undefined}
      protocolVersion={protocolVersion}
    />
  );
}
