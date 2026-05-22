export type PublicObjectStorageProvider = 'gcs';

export type PublicObjectStorageTableRoot = {
  provider: PublicObjectStorageProvider;
  tableUri: string;
  bucket: string;
  prefix: string;
  tableRootUrl: string;
};

export type PublicObjectStorageErrorCode =
  | 'invalid_public_object_storage_uri'
  | 'invalid_public_object_path';

export class PublicObjectStorageError extends Error {
  readonly code: PublicObjectStorageErrorCode;

  constructor(code: PublicObjectStorageErrorCode, message: string) {
    super(message);
    this.name = 'PublicObjectStorageError';
    this.code = code;
  }
}

export function parsePublicObjectStorageTableRoot(input: {
  provider: PublicObjectStorageProvider;
  tableUri: string;
}): PublicObjectStorageTableRoot {
  if (input.provider !== 'gcs') {
    throw invalidUri('public object storage currently supports only GCS table roots');
  }

  const trimmed = input.tableUri.trim().replace(/\/+$/, '');
  if (containsSecretMaterial(trimmed)) {
    throw invalidUri('public object storage table URI must not contain credential material');
  }

  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch (error) {
    throw invalidUri(
      `invalid public object storage table URI: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

  if (
    parsed.protocol !== 'gs:' ||
    !parsed.hostname ||
    hasUserinfo(parsed) ||
    parsed.search ||
    parsed.hash
  ) {
    throw invalidUri('public object storage table URI must look like gs://bucket/table');
  }

  const prefix = normalizeObjectPath(parsed.pathname);
  if (!prefix) {
    throw invalidUri('public object storage table URI must include a table path');
  }

  const bucket = parsed.hostname;
  return {
    provider: input.provider,
    tableUri: `gs://${bucket}/${prefix}`,
    bucket,
    prefix,
    tableRootUrl: `https://storage.googleapis.com/${encodeObjectPath(bucket)}/${encodeObjectPath(
      prefix,
    )}/`,
  };
}

export function publicObjectUrl(root: PublicObjectStorageTableRoot, relativePath: string): string {
  if (relativePath.startsWith('/')) {
    throw invalidPath('public object relative path must stay inside the table root');
  }
  const normalized = normalizeObjectPath(relativePath);
  if (!normalized || normalized !== relativePath.replace(/^\/+|\/+$/g, '')) {
    throw invalidPath('public object relative path must stay inside the table root');
  }
  return `${root.tableRootUrl}${encodeObjectPath(normalized)}`;
}

function normalizeObjectPath(path: string): string {
  const parts = path.split('/').filter(Boolean);
  if (parts.some((part) => part === '.' || part === '..')) {
    throw invalidPath('public object relative path must not contain traversal segments');
  }
  return parts.join('/');
}

function encodeObjectPath(path: string): string {
  return path.split('/').map(encodeURIComponent).join('/');
}

function hasUserinfo(url: URL): boolean {
  return Boolean(url.username || url.password);
}

function containsSecretMaterial(value: string): boolean {
  const lower = value.toLowerCase();
  return (
    /akia[0-9a-z]{16}/i.test(value) ||
    lower.includes('x-goog-signature') ||
    lower.includes('x-goog-credential') ||
    lower.includes('google_application_credentials') ||
    lower.includes('private_key') ||
    lower.includes('access_token') ||
    lower.includes('bearer')
  );
}

function invalidUri(message: string): PublicObjectStorageError {
  return new PublicObjectStorageError('invalid_public_object_storage_uri', message);
}

function invalidPath(message: string): PublicObjectStorageError {
  return new PublicObjectStorageError('invalid_public_object_path', message);
}
