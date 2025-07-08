import { S3Client } from "@aws-sdk/client-s3";
import { FMConfig, Logger, WithSpanFn } from "../types/fmconfig-types.js";
import { isValidLogger } from "../utils/isValidLogger.js";
import { Readable } from "stream";
import { StreamType } from "../utils/type-guards.js";
import { Stream } from "../types/input-types.js";

const MAX_ATTEMPTS_DEFAULT = 3;
const MULTIPART_THRESHOLD_DEFAULT = 10 * 1024 * 1024;
const MULTIPART_THRESHOLD_MIN = 5 * 1024 * 1024;

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
        } else {
            const arrayBuffer = await (stream as Blob).arrayBuffer();
            return Buffer.from(arrayBuffer);
        }
    }
}
