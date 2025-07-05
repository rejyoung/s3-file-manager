import {
    AbortMultipartUploadCommand,
    CompletedPart,
    CompleteMultipartUploadCommand,
    CreateMultipartUploadCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    PutObjectCommand,
    S3Client,
    UploadPartCommand,
} from "@aws-sdk/client-s3";
import { FMConfig, Logger, WithSpanFn } from "./types/fmconfig-types.js";
import { isValidLogger } from "./utils/isValidLogger.js";
import {
    ConfirmFilesOptions,
    FilePayload,
    FileUploadOptions,
    ListFilesOptions,
    Stream,
} from "./types/input-types.js";
import { backoffDelay, wait } from "./utils/wait.js";
import { Readable } from "stream";
import { isStreamType, StreamType } from "./utils/type-guards.js";
import { UploadContentType } from "./types/internal-types.js";
import { lookup as mimeLookup } from "mime-types";

const MAX_ATTEMPTS_DEFAULT = 3;
const MULTIPART_THRESHOLD_DEFAULT = 10 * 1024 * 1024;
const MULTIPART_THRESHOLD_MIN = 5 * 1024 * 1024;

export class S3FileManager {
    private bucketName: string;
    private s3: S3Client;
    private logger: Logger;
    private withSpan: WithSpanFn;
    private maxAttempts: number;
    private attemptNumString: string;
    private multipartThreshold: number;
    private allowVerboseLogging: boolean;

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
    // 📂 LIST FILES
    // Retrieves files, passing in an optional prefix,
    // and then filters and sorts them according to user-supplied functions.
    // If no comparison function is supplied, sort uses the default lexicographic method.
    // ════════════════════════════════════════════════════════════════

    public async listFiles(options: ListFilesOptions = {}): Promise<string[]> {
        const {
            prefix,
            filterFn = (fileName: string) => true,
            compareFn = undefined,
            spanOptions = {},
        } = options;

        const {
            name: spanName = "S3FileManager.listFiles",
            attributes: spanAttributes = {
                bucket: this.bucketName,
                prefix: prefix ?? "",
            },
        } = spanOptions;

        const params = {
            Bucket: this.bucketName,
            Prefix: prefix,
        };

        const result = await this.withSpan(
            spanName,
            spanAttributes,
            async () => {
                let attempt = 0;

                while (true) {
                    try {
                        attempt++;
                        const command = new ListObjectsV2Command(params);
                        const response = await this.s3.send(command);

                        const files =
                            response.Contents?.map((file) => file.Key || "")
                                .filter(Boolean)
                                .filter(filterFn) || [];

                        const sortedFiles = files?.sort(compareFn);

                        this.verboseLog(
                            `Successfully retrieved ${files.length} file(s)${
                                params.Prefix
                                    ? ` with prefix '${params.Prefix}'`
                                    : ""
                            }'`
                        );

                        return sortedFiles;
                    } catch (error) {
                        this.handleRetryErrorLogging(
                            attempt,
                            `to fetch list of files`,
                            error
                        );

                        // Wait before next attempt
                        await wait(backoffDelay(attempt));
                    }
                }
            }
        );
        return result;
    }

    // ════════════════════════════════════════════════════════════════
    // ✅ CONFIRM FILE EXISTENCE
    // Verifies the presence of specified files in the S3 bucket using HeadObject
    // ════════════════════════════════════════════════════════════════
    public async confirmFilesExist(
        options: ConfirmFilesOptions
    ): Promise<{ allExist: boolean; missingFiles: string[] }> {
        const { prefix, filenames, spanOptions = {} } = options;

        const missingFiles: string[] = [];

        await Promise.all(
            filenames.map(async (filename) => {
                const {
                    name: spanName = "S3FileManager.confirmFilesExist",
                    attributes: spanAttributes = {
                        bucket: this.bucketName,
                        filename: `${prefix ?? ""}${filename}`,
                    },
                } = spanOptions;

                await this.withSpan(spanName, spanAttributes, async () => {
                    let attempt = 0;
                    let success = false;

                    while (!success) {
                        try {
                            attempt++;
                            const command = new HeadObjectCommand({
                                Bucket: this.bucketName,
                                Key: `${prefix ?? ""}${filename}`,
                            });
                            await this.s3.send(command);

                            success = true;
                        } catch (error: any) {
                            if (
                                error.name === "NotFound" ||
                                error.$metadata?.httpStatusCode === 404
                            ) {
                                this.verboseLog(
                                    `${filename} not found in bucket ${this.bucketName}.`
                                );
                                missingFiles.push(filename);
                                success = true;
                            } else {
                                this.handleRetryErrorLogging(
                                    attempt,
                                    `to verify the existence of file ${filename}`,
                                    error
                                );
                                // Wait before next attempt
                                await wait(backoffDelay(attempt));
                            }
                        }
                    }
                });
            })
        );

        const result = {
            allExist: missingFiles.length === 0,
            missingFiles,
        };

        this.verboseLog(
            `Checked ${filenames.length} file(s); missing: ${missingFiles.length}`
        );

        return result;
    }

    // ════════════════════════════════════════════════════════════════
    // 🔼 UPLOAD FILE
    // Routes file upload requests to the appropriate upload function based on size
    // ════════════════════════════════════════════════════════════════

    public async uploadFile(
        file: FilePayload,
        options: FileUploadOptions
    ): Promise<void> {
        const { content } = file;
        try {
            let sizeInBytes: number | undefined;
            let type: UploadContentType;

            if (typeof content === "string") {
                sizeInBytes =
                    options.sizeHint ?? Buffer.byteLength(content, "utf-8");
                type = "string";
            } else if (Buffer.isBuffer(content)) {
                sizeInBytes = options.sizeHint ?? content.length;
                type = "Buffer";
            } else if (content instanceof Uint8Array) {
                sizeInBytes = options.sizeHint ?? content.byteLength;
                type = "Uint8Array";
            } else if (typeof Blob !== "undefined" && content instanceof Blob) {
                sizeInBytes = options.sizeHint ?? content.size;
                type = "Blob";
            } else if (content instanceof Readable) {
                sizeInBytes = options.sizeHint ?? undefined;
                type = "Readable";
            } else {
                sizeInBytes = options.sizeHint ?? undefined;
                type = "ReadableStream";
            }

            this.verboseLog(
                `Uploading ${file.name} (${sizeInBytes} bytes) using ${
                    sizeInBytes && sizeInBytes > this.multipartThreshold
                        ? "multipart"
                        : "simple"
                } upload`
            );

            const mimeType =
                options.contentType ??
                (mimeLookup(file.name) || "application/octet-stream");

            if (sizeInBytes && sizeInBytes > this.multipartThreshold) {
                await this.multipartUpload(
                    file,
                    options,
                    type,
                    mimeType,
                    sizeInBytes
                );
            } else {
                await this.simpleUpload(file, options, mimeType);
            }
        } catch (error) {}
    }

    // ════════════════════════════════════════════════════════════════
    // 📤 SIMPLE UPLOAD
    // Uploads small files in a single PUT request
    // ════════════════════════════════════════════════════════════════

    private async simpleUpload(
        file: FilePayload,
        options: FileUploadOptions,
        mimeType: string
    ): Promise<void> {
        const { prefix, spanOptions = {} } = options;
        const {
            name: spanName = "S3FileManager.simpleUpload",
            attributes: spanAttributes = {
                bucket: this.bucketName,
                filename: `${prefix ?? ""}${file.name}`,
            },
        } = spanOptions;

        const key = `${prefix ?? ""}${file.name}`;
        let attempt = 0;

        await this.withSpan(spanName, spanAttributes, async () => {
            while (true) {
                try {
                    attempt++;
                    const command = new PutObjectCommand({
                        Bucket: this.bucketName,
                        Key: key,
                        Body: file.content,
                        ContentType: mimeType,
                    });
                    await this.s3.send(command);
                    this.verboseLog(`Successfully uploaded ${file.name}`);
                    return;
                } catch (error) {
                    this.handleRetryErrorLogging(
                        attempt,
                        `to upload ${file.name}`,
                        error
                    );

                    // Wait before next attempt
                    await wait(backoffDelay(attempt));
                }
            }
        });
    }

    // ════════════════════════════════════════════════════════════════
    // 🧱 MULTIPART UPLOAD
    // Splits large content into parts and uploads via multipart API
    // ════════════════════════════════════════════════════════════════

    private async multipartUpload(
        file: FilePayload,
        options: FileUploadOptions,
        type: UploadContentType,
        mimeType: string,
        size: number
    ): Promise<boolean> {
        const { prefix, spanOptions = {} } = options;

        const {
            name: spanName = "S3FileManager.multipartUpload",
            attributes: spanAttributes = {
                bucket: this.bucketName,
                filename: `${prefix ?? ""}${file.name}`,
            },
        } = spanOptions;

        let fileChunks: AsyncIterable<Buffer> | Array<Buffer>;
        try {
            if (isStreamType(type) && (!size || size > 200 * 1024 * 1024)) {
                fileChunks = await this.streamToIterable(
                    file.content as Stream,
                    type
                );
                this.verboseLog(
                    "Successfully prepared stream for multipart upload."
                );
            } else {
                let buffer: Buffer;
                if (isStreamType(type)) {
                    buffer = await this.streamToBuffer(
                        file.content as Stream,
                        type
                    );
                } else if (type === "string") {
                    buffer = Buffer.from(file.content as string);
                } else if (type === "Uint8Array") {
                    buffer = Buffer.from(file.content as Uint8Array);
                } else {
                    buffer = file.content as Buffer;
                }
                fileChunks = this.bufferToChunks(buffer);
                this.verboseLog(
                    "Successfully constructed Buffer for multipart upload."
                );
            }
        } catch (error) {
            throw new Error(
                `Something went wrong while attempting to prepare content for multipart upload: ${this.errorString(
                    error
                )}`,
                { cause: error }
            );
        }

        const response = await this.withSpan(
            spanName,
            spanAttributes,
            async () => {
                let uploadId: string | undefined;
                const filename = `${prefix ?? ""}${file.name}`;

                let attempt = 0;
                while (true) {
                    attempt++;
                    try {
                        const createResponse = await this.s3.send(
                            new CreateMultipartUploadCommand({
                                Bucket: this.bucketName,
                                Key: filename,
                                ContentType: mimeType,
                            })
                        );

                        uploadId = createResponse.UploadId;
                        if (!uploadId)
                            throw new Error(
                                "Failed to initiate multipart upload"
                            );

                        const parts: CompletedPart[] = [];
                        let partNumber = 1;

                        for await (const chunk of fileChunks) {
                            const ETag = await this.uploadPartWithRetry(
                                filename,
                                uploadId,
                                partNumber,
                                chunk
                            );
                            parts.push({
                                ETag,
                                PartNumber: partNumber,
                            });

                            partNumber++;
                        }

                        await this.s3.send(
                            new CompleteMultipartUploadCommand({
                                Bucket: this.bucketName,
                                Key: filename,
                                UploadId: uploadId,
                                MultipartUpload: { Parts: parts },
                            })
                        );
                        this.verboseLog(
                            `File ${filename} successfully uploaded to S3 bucket`
                        );
                        return true;
                    } catch (error) {
                        await this.s3.send(
                            new AbortMultipartUploadCommand({
                                Bucket: this.bucketName,
                                Key: filename,
                                UploadId: uploadId,
                            })
                        );

                        this.handleRetryErrorLogging(
                            attempt,
                            `to upload ${filename}`,
                            error
                        );

                        // Wait before next attempt
                        await wait(backoffDelay(attempt));
                    }
                }
            }
        );
        return response;
    }

    private async uploadPartWithRetry(
        filename: string,
        uploadId: string,
        partNumber: number,
        chunk: Buffer
    ): Promise<string> {
        let attempt = 0;
        while (true) {
            try {
                attempt++;
                const uploadPartResponse = await this.s3.send(
                    new UploadPartCommand({
                        Bucket: this.bucketName,
                        Key: filename,
                        PartNumber: partNumber,
                        UploadId: uploadId,
                        Body: chunk,
                    })
                );
                return uploadPartResponse.ETag!;
            } catch (error) {
                this.handleRetryErrorLogging(
                    attempt,
                    `to upload part ${partNumber} of ${filename}`,
                    error
                );

                // Wait before next attempt
                await wait(backoffDelay(attempt));
            }
        }
    }

    // --- 🔄 Stream to Buffer Conversion-----------------------------------------
    private async streamToBuffer(
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

    private async *streamToIterable(
        stream: Stream,
        type: StreamType
    ): AsyncIterable<Buffer> {
        let buffer = Buffer.alloc(0);
        if (type === "Readable") {
            for await (const chunk of stream as Readable) {
                buffer = Buffer.concat([
                    buffer,
                    Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk),
                ]);
                while (buffer.length >= this.multipartThreshold) {
                    yield buffer.subarray(0, this.multipartThreshold);
                    buffer = buffer.subarray(this.multipartThreshold);
                }
                if (buffer.length) yield buffer;
            }
        } else {
            const preparedStream =
                type === "Blob"
                    ? (stream as Blob).stream()
                    : (stream as ReadableStream);
            const reader = preparedStream.getReader();
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buffer = Buffer.concat([buffer, Buffer.from(value)]);
                while (buffer.length >= this.multipartThreshold) {
                    yield buffer.subarray(0, this.multipartThreshold);
                    buffer = buffer.subarray(this.multipartThreshold);
                }
            }
            if (buffer.length) yield buffer;
        }
    }

    private bufferToChunks(buffer: Buffer): Array<Buffer> {
        const bufferChunks: Array<Buffer> = [];
        const totalFileSize = buffer.length;
        const numberOfParts = Math.ceil(
            totalFileSize / this.multipartThreshold
        );

        for (let part = 1; part <= numberOfParts; part++) {
            const start = (part - 1) * this.multipartThreshold;
            const end = Math.min(
                start + this.multipartThreshold,
                totalFileSize
            );
            const chunk = buffer.subarray(start, end);
            bufferChunks.push(chunk);
        }
        return bufferChunks;
    }

    // ════════════════════════════════════════════════════════════════
    // 🛠 UTILITIES
    // General helper functions for internal use
    // ════════════════════════════════════════════════════════════════

    private errorString(err: any) {
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

    private handleRetryErrorLogging(
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

    private verboseLog(message: string) {
        if (this.allowVerboseLogging) {
            this.logger.info(message);
        }
    }
}

// public async uploadAuto(
//   file: { name: string; content: string },
//   folder: string
// ): Promise<void> {
//   // Decide threshold in bytes (e.g. 5 MB)
//   const MULTIPART_THRESHOLD = 5 * 1024 * 1024;

//   // Compute the byte length of the content
//   const sizeInBytes = Buffer.byteLength(file.content, "utf-8");

//   if (sizeInBytes > MULTIPART_THRESHOLD) {
//     // Large: use your multipart path
//     return this.uploadLargeFile(file, folder);
//   } else {
//     // Small: single PutObject call
//     const key = `${folder}/${file.name}`;
//     const command = new PutObjectCommand({
//       Bucket: this.bucketName,
//       Key: key,
//       Body: file.content,
//       ContentType: "application/json", // or whatever fits
//     });
//     await this.withSpan(
//       "S3FileManager.uploadAuto",
//       { bucket: this.bucketName, key, size: sizeInBytes },
//       () => this.s3.send(command)
//     );
//   }
// }
