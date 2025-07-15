import { ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import { FMConfig, Logger, WithSpanFn } from "../types/fmconfig-types.js";
import { isValidLogger } from "../utils/isValidLogger.js";
import { Readable } from "stream";
import { StreamType } from "../utils/type-guards.js";
import { Stream } from "../types/input-types.js";
import { backoffDelay, wait } from "../utils/wait.js";
import { ListItemsOptionsInternal } from "../types/internal-types.js";
import { formatPrefix } from "../utils/formatPrefix.js";

const MAX_ATTEMPTS_DEFAULT = 3;
const DEFAULT_UPLOAD_PART_SIZE = 10 * 1024 * 1024;
const MIN_UPLOAD_PART_SIZE = 5 * 1024 * 1024;
const MAX_UPLOAD_PART_SIZE = 100 * 1024 * 1024;

/**
 ╔════════════════════════════════════════════════════════════════╗
 ║ ☁️ S3 CLIENT WRAPPER                                           ║
 ║ Wraps the S3 client and exposes shared utilities and methods.  ║
 ╚════════════════════════════════════════════════════════════════╝
 */
export class S3FMContext {
    public readonly bucketName: string;
    public readonly s3: S3Client;
    public readonly logger: Logger;
    public readonly withSpan: WithSpanFn;
    public readonly maxAttempts: number;
    public readonly attemptNumString: string;
    public readonly maxUploadPartSize: number;
    public readonly allowVerboseLogging: boolean;

    constructor(config: FMConfig) {
        this.bucketName = config.bucketName;

        this.s3 = new S3Client({
            region: config.bucketRegion,
            endpoint: config.endpoint,
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

        // Convert input maxUploadPartSizeMB to bytes or set to zero if undefined
        const userMaxPartSize = config.maxUploadPartSizeMB
            ? config.maxUploadPartSizeMB * 1024 * 1024
            : 0;
        this.maxUploadPartSize =
            userMaxPartSize > MIN_UPLOAD_PART_SIZE &&
            userMaxPartSize < MAX_UPLOAD_PART_SIZE
                ? userMaxPartSize
                : DEFAULT_UPLOAD_PART_SIZE;

        this.allowVerboseLogging = config.verboseLogging ?? false;
    }

    // ════════════════════════════════════════════════════════════════
    // 📂 LIST ITEMS
    // General-purpose function for listing files or directories
    // ════════════════════════════════════════════════════════════════

    public async listItems(
        prefix: string,
        options: ListItemsOptionsInternal
    ): Promise<string[]> {
        const {
            filterFn = (fileName: string) => true,
            compareFn = undefined,
            directoriesOnly = false,
            spanOptions = {},
        } = options;

        const { name: spanName, attributes: spanAttributes } = spanOptions;

        const params = {
            Bucket: this.bucketName,
            Prefix: prefix ? formatPrefix("", prefix) : undefined,
            Delimiter: directoriesOnly ? "/" : undefined,
        };

        const result = await this.withSpan(
            spanName ?? "S3FileManager.listItems",
            spanAttributes ?? { bucket: this.bucketName, prefix },
            async () => {
                const filteredItems: string[] = [];

                let continuationToken: string | undefined = undefined;

                do {
                    let attempt = 0;
                    let response;
                    while (true) {
                        try {
                            attempt++;
                            const command = new ListObjectsV2Command(params);
                            response = await this.s3.send(command);
                            break;
                        } catch (error) {
                            this.handleRetryErrorLogging(
                                attempt,
                                `to fetch list of ${
                                    directoriesOnly ? "directories" : "files"
                                }`,
                                error
                            );

                            // Wait before next attempt
                            await wait(backoffDelay(attempt));
                        }
                    }
                    let items: string[];
                    if (directoriesOnly) {
                        items =
                            response.CommonPrefixes?.map((p) => p.Prefix || "")
                                .filter(Boolean)
                                .filter(filterFn) || [];
                    } else {
                        items =
                            response.Contents?.map((file) => file.Key || "")
                                .filter(Boolean)
                                .filter(filterFn) || [];
                    }

                    filteredItems.push(...items);
                    continuationToken = response.NextContinuationToken;
                    (params as any).ContinuationToken = continuationToken;
                } while (continuationToken);

                const sortedItems = filteredItems.sort(compareFn);

                this.verboseLog(
                    `Successfully retrieved ${sortedItems.length} ${
                        directoriesOnly
                            ? `director${
                                  sortedItems.length === 1 ? "y" : "ies"
                              }`
                            : "file(s)"
                    }${params.Prefix ? ` with prefix '${params.Prefix}'` : ""}'`
                );

                return sortedItems;
            }
        );
        return result;
    }

    // ════════════════════════════════════════════════════════════════
    // 📊 WITH SPAN
    // Wraps a function in a tracing span for observability
    // ════════════════════════════════════════════════════════════════
    public errorString(err: any) {
        return err instanceof Error ? err.message : String(err);
    }

    // ════════════════════════════════════════════════════════════════
    // 🔎 GET TRACER
    // Returns the tracer instance for span creation
    // ════════════════════════════════════════════════════════════════
    private logRetryWarning(attempt: number, action: string, error: any) {
        this.logger.warn(
            `Attempt ${attempt} of ${
                this.maxAttempts
            } ${action} failed: ${this.errorString(error)}`
        );
        this.logger.warn("Retrying...");
    }

    // ════════════════════════════════════════════════════════════════
    // 📝 GET LOGGER
    // Returns the logger instance for structured logging
    // ════════════════════════════════════════════════════════════════
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

    // ════════════════════════════════════════════════════════════════
    // 🗯 VERBOSE LOG
    // Outputs a verbose-level log message if verbosity is enabled
    // ════════════════════════════════════════════════════════════════
    public verboseLog(message: string, type?: "info" | "warn" | "error") {
        if (this.allowVerboseLogging) {
            switch (type) {
                case "info":
                    this.logger.info(message);
                    break;
                case "warn":
                    this.logger.warn(message);
                    break;
                case "error":
                    this.logger.error(message);
                    break;
                default:
                    this.logger.info(message);
            }
        }
    }

    // ════════════════════════════════════════════════════════════════
    // 🔁 STREAM TO BUFFER
    // Converts a readable stream into a buffer
    // ════════════════════════════════════════════════════════════════
    public async streamToBuffer(
        stream: Stream,
        type: StreamType
    ): Promise<Buffer> {
        const chunks: Buffer[] = [];
        if (type === "Readable") {
            for await (const chunk of stream as Readable) {
                chunks.push(
                    Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
                );
            }
            return Buffer.concat(chunks);
        } else if (type === "ReadableStream") {
            const reader = (stream as ReadableStream).getReader();
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                chunks.push(Buffer.from(value));
            }
            return Buffer.concat(chunks);
        } else if (stream instanceof Blob) {
            const arrayBuffer = await (stream as Blob).arrayBuffer();
            return Buffer.from(arrayBuffer);
        } else {
            throw new Error("Unsupported stream type");
        }
    }
}
