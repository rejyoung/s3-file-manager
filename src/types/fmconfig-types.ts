import type { Provider, AwsCredentialIdentity } from "@aws-sdk/types";

export interface FMConfig {
    bucketName: string;
    bucketRegion: string;
    endpoint?: string; // Optional, for S3-compatible services (e.g., MinIO, R2)
    credentials?: AwsCredentialIdentity | Provider<AwsCredentialIdentity>;
    forcePathStyle?: boolean;
    maxAttempts?: number;
    maxUploadPartSizeMB?: number;
    logger?: Logger;
    verboseLogging?: boolean;
    withSpan?: WithSpanFn;
    maxUploadConcurrency?: number;
}

// Optional custom logging plugin
export interface Logger {
    info(message: string, meta?: any): void;
    warn(message: string, meta?: any): void;
    error(message: string, meta?: any): void;
}

// Optional tracer plugin
export type WithSpanFn = <T>(
    name: string,
    metadata: Record<string, any>,
    fn: () => Promise<T>
) => Promise<T>;
