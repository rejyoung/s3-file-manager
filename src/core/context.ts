import { S3Client } from "@aws-sdk/client-s3";
import { FMConfig, Logger, WithSpanFn } from "../types/fmconfig-types.js";
import { isValidLogger } from "../utils/isValidLogger.js";

const MAX_ATTEMPTS_DEFAULT = 3;
const MULTIPART_THRESHOLD_DEFAULT = 10 * 1024 * 1024;
const MULTIPART_THRESHOLD_MIN = 5 * 1024 * 1024;

export class S3FMContext {
    public readonly bucketName: string;
    public readonly s3: S3Client;
    public readonly logger: Logger;
    public readonly withSpan: WithSpanFn;
    public readonly maxAttempts: number;
    public readonly attemptNumString: string;
    public readonly multipartThreshold: number;
    public readonly allowVerboseLogging: boolean;

    constructor(config: FMConfig) {
        this.bucketName = config.bucketName;

        this.s3 = new S3Client({
            region: config.bucketRegion,
            endpoint: config.endpoint ?? undefined,
            credentials: config.credentials,
            forcePathStyle: config.forcePathStyle,
        });

        this.logger = isValidLogger(config.logger)
            ? config.logger
            : { info: console.log, warn: console.warn, error: console.error };

        this.withSpan =
            config.withSpan ?? (async (_n, _m, work) => await work());

        this.maxAttempts = config.maxAttempts ?? MAX_ATTEMPTS_DEFAULT;
        this.attemptNumString = `${this.maxAttempts} attempt${
            this.maxAttempts > 1 ? "s" : ""
        }`;

        this.multipartThreshold =
            config.multipartThreshold &&
            config.multipartThreshold > MULTIPART_THRESHOLD_MIN
                ? config.multipartThreshold
                : MULTIPART_THRESHOLD_DEFAULT;

        this.allowVerboseLogging = config.verboseLogging ?? false;
    }

    // ════════════════════════════════════════════════════════════════
    // 🛠 UTILITIES
    // General helper functions for internal use
    // ════════════════════════════════════════════════════════════════

    public errorString(err: any) {
        return err instanceof Error ? err.message : String(err);
    }

    private logRetryWarning(attempt: number, action: string, error: any) {
        this.logger.warn(
            `Attempt ${attempt} of ${
                this.maxAttempts
            } ${action} failed: ${this.errorString(error)}`
        );
        this.logger.warn("Retrying...");
    }

    public handleRetryErrorLogging(
        attempt: number,
        action: string,
        error: any
    ) {
        if (attempt === this.maxAttempts) {
            throw new Error(
                `Failed ${action} ${this.attemptNumString}: ${this.errorString(
                    error
                )}`,
                { cause: error }
            );
        }

        this.logRetryWarning(attempt, action, error);
    }

    public verboseLog(message: string) {
        if (this.allowVerboseLogging) {
            this.logger.info(message);
        }
    }
}
