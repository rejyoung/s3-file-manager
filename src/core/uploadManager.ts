import {
    AbortMultipartUploadCommand,
    CompletedPart,
    CompleteMultipartUploadCommand,
    CreateMultipartUploadCommand,
    PutObjectCommand,
    UploadPartCommand,
} from "@aws-sdk/client-s3";
import { Readable } from "stream";
import { S3FMContext } from "./context.js";
import { FilePayload, UploadOptions, Stream } from "../types/input-types.js";
import { UploadContentType } from "../types/internal-types.js";
import { wait } from "../utils/wait.js";
import { backoffDelay } from "../utils/wait.js";
import { isStreamType, StreamType } from "../utils/type-guards.js";
import { lookup as mimeLookup } from "mime-types";
import Bottleneck from "bottleneck";
import path from "path";
import { UploadFilesReturnType } from "../types/return-types.js";

/**
 ╔════════════════════════════════════════════════════════════════════════════════╗
 ║ 📤 UPLOAD MANAGER                                                              ║
 ║ Handles file uploads to S3, including direct uploads, multipart uploads, and   ║
 ║ retry logic for reliability.                                                   ║
 ╚════════════════════════════════════════════════════════════════════════════════╝
 */
export class UploadManager {
    private ctx: S3FMContext;
    private maxConcurrent: number;
    private limiter: Bottleneck;

    constructor(context: S3FMContext, maxUploadConcurrency?: number) {
        this.ctx = context;
        this.maxConcurrent = maxUploadConcurrency ?? 4;
        this.limiter = new Bottleneck({ maxConcurrent: this.maxConcurrent });
    }
    // ════════════════════════════════════════════════════════════════
    // 🔼 UPLOAD FILE
    // Routes file upload requests to the appropriate upload function based on size
    // ════════════════════════════════════════════════════════════════

    public async uploadFile(
        file: FilePayload,
        options: UploadOptions = {}
    ): Promise<string> {
        const { content, sizeHintBytes, contentType } = file;
        const { spanOptions = {}, prefix } = options;

        const {
            name: spanName = "S3FileManager.uploadFile",
            attributes: spanAttributes = {
                bucket: this.ctx.bucketName,
                filename: `${prefix ?? ""}${file.name}`,
            },
        } = spanOptions;

        const result = await this.ctx.withSpan(
            spanName,
            spanAttributes,
            async () => {
                let sizeInBytes: number | undefined;
                let type: UploadContentType;

                if (typeof content === "string") {
                    sizeInBytes =
                        sizeHintBytes ?? Buffer.byteLength(content, "utf-8");
                    type = "string";
                } else if (Buffer.isBuffer(content)) {
                    sizeInBytes = sizeHintBytes ?? content.length;
                    type = "Buffer";
                } else if (content instanceof Uint8Array) {
                    sizeInBytes = sizeHintBytes ?? content.byteLength;
                    type = "Uint8Array";
                } else if (
                    typeof Blob !== "undefined" &&
                    content instanceof Blob
                ) {
                    sizeInBytes = sizeHintBytes ?? content.size;
                    type = "Blob";
                } else if (content instanceof Readable) {
                    sizeInBytes = sizeHintBytes ?? undefined;
                    type = "Readable";
                } else {
                    sizeInBytes = sizeHintBytes ?? undefined;
                    type = "ReadableStream";
                }

                this.ctx.verboseLog(
                    `Uploading ${file.name} (${sizeInBytes} bytes) using ${
                        sizeInBytes && sizeInBytes > this.ctx.maxUploadPartSize
                            ? "multipart"
                            : "simple"
                    } upload`
                );

                const mimeType =
                    contentType ??
                    (mimeLookup(file.name) || "application/octet-stream");

                if (
                    sizeInBytes === undefined ||
                    sizeInBytes > this.ctx.maxUploadPartSize
                ) {
                    return await this.multipartUpload(
                        file,
                        type,
                        mimeType,
                        sizeInBytes,
                        options.prefix
                    );
                } else {
                    return await this.simpleUpload(
                        file,
                        mimeType,
                        options.prefix
                    );
                }
            }
        );
        return result;
    }

    // ════════════════════════════════════════════════════════════════
    // 📦 UPLOAD MULTIPLE FILES
    // Iterates through arrays of files and uploads them with predefined concurrency limits
    // ════════════════════════════════════════════════════════════════

    public async uploadMultipleFiles(
        files: FilePayload[],
        options: UploadOptions = {}
    ): Promise<UploadFilesReturnType> {
        // Limiter to prevent race conditions when pushing to skippedFiles and filePaths arrays.
        const mutex = new Bottleneck({ maxConcurrent: 1 });

        const { spanOptions = {}, prefix } = options;

        const {
            name: spanName = "S3FileManager.uploadMultipleFiles",
            attributes: spanAttributes = {
                bucket: this.ctx.bucketName,
            },
        } = spanOptions;

        let skippedFiles: string[] = [];
        let filePaths: string[] = [];

        await this.ctx.withSpan(spanName, spanAttributes, async () => {
            await Promise.all(
                files.map(async (file) => {
                    try {
                        const result = await this.uploadFile(file, { prefix });
                        await mutex.schedule(async () => {
                            filePaths.push(result);
                        });
                    } catch (error) {
                        await mutex.schedule(async () =>
                            skippedFiles.push(file.name)
                        );
                        this.ctx.verboseLog(String(error), "warn");
                        this.ctx.verboseLog("Skipping file...", "warn");
                    }
                })
            );
        });

        if (skippedFiles.length > 0) {
            this.ctx.logger.warn(
                `File batch upload finished, but the following ${
                    skippedFiles.length
                } file(s) failed to upload: ${skippedFiles.join(", ")}`
            );
        } else {
            this.ctx.logger.info(
                "File batch upload complete. All files successfully uploaded."
            );
        }

        return { filePaths, skippedFiles };
    }

    // ════════════════════════════════════════════════════════════════
    // 📤 SIMPLE UPLOAD
    // Uploads small files in a single PUT request
    // ════════════════════════════════════════════════════════════════

    private async simpleUpload(
        file: FilePayload,
        mimeType: string,
        prefix?: string
    ): Promise<string> {
        const key = `${prefix ?? ""}${file.name}`;
        let attempt = 0;

        const result = await this.ctx.withSpan(
            "S3FileManager.uploadFile > simpleUpload",
            {
                bucket: this.ctx.bucketName,
                filename: `${prefix ?? ""}${file.name}`,
            },
            async () => {
                while (true) {
                    try {
                        attempt++;
                        const command = new PutObjectCommand({
                            Bucket: this.ctx.bucketName,
                            Key: key,
                            Body: file.content,
                            ContentType: mimeType,
                        });
                        await this.limiter.schedule(() =>
                            this.ctx.s3.send(command)
                        );
                        this.ctx.verboseLog(
                            `Successfully uploaded ${file.name}`
                        );
                        return `${prefix ?? ""}${file.name}`;
                    } catch (error) {
                        this.ctx.handleRetryErrorLogging(
                            attempt,
                            `to upload ${file.name}`,
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
    // 🧱 MULTIPART UPLOAD
    // Splits large content into parts and uploads via multipart API
    // ════════════════════════════════════════════════════════════════

    private async multipartUpload(
        file: FilePayload,
        type: UploadContentType,
        mimeType: string,
        size?: number,
        prefix?: string
    ): Promise<string> {
        let fileChunks: AsyncIterable<Buffer> | Buffer[];
        try {
            if (isStreamType(type) && (!size || size > 200 * 1024 * 1024)) {
                fileChunks = await this.streamToIterable(
                    file.content as Stream,
                    type
                );
                this.ctx.verboseLog(
                    "Successfully prepared stream for multipart upload."
                );
            } else {
                let buffer: Buffer;
                if (isStreamType(type)) {
                    buffer = await this.ctx.streamToBuffer(
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
                this.ctx.verboseLog(
                    "Successfully constructed Buffer for multipart upload."
                );
            }
        } catch (error) {
            throw new Error(
                `Something went wrong while attempting to prepare content for multipart upload: ${this.ctx.errorString(
                    error
                )}`,
                { cause: error }
            );
        }

        const response = await this.ctx.withSpan(
            "S3FileManager.uploadFile > multipartUpload",
            {
                bucket: this.ctx.bucketName,
                filename: `${prefix ?? ""}${file.name}`,
            },
            async () => {
                let uploadId: string | undefined;
                const filename = `${prefix ?? ""}${file.name}`;

                let attempt = 0;
                while (true) {
                    attempt++;
                    try {
                        const createResponse = await this.ctx.s3.send(
                            new CreateMultipartUploadCommand({
                                Bucket: this.ctx.bucketName,
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

                        await this.ctx.s3.send(
                            new CompleteMultipartUploadCommand({
                                Bucket: this.ctx.bucketName,
                                Key: filename,
                                UploadId: uploadId,
                                MultipartUpload: { Parts: parts },
                            })
                        );
                        this.ctx.verboseLog(
                            `File ${filename} successfully uploaded to S3 bucket`
                        );
                        return `${prefix ?? ""}${file.name}`;
                    } catch (error) {
                        await this.ctx.s3.send(
                            new AbortMultipartUploadCommand({
                                Bucket: this.ctx.bucketName,
                                Key: filename,
                                UploadId: uploadId,
                            })
                        );

                        this.ctx.handleRetryErrorLogging(
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

    // ════════════════════════════════════════════════════════════════
    // 🔁 UPLOAD PART WITH RETRY
    // Uploads individual chunks of a multipart upload with retry logic
    // ════════════════════════════════════════════════════════════════

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
                const uploadPartResponse = await this.ctx.withSpan(
                    "S3FileManager.uploadFile > multipartUpload > uploadPartWithRetry",
                    { filename, uploadId, partNumber },
                    async () =>
                        await this.limiter.schedule(() =>
                            this.ctx.s3.send(
                                new UploadPartCommand({
                                    Bucket: this.ctx.bucketName,
                                    Key: filename,
                                    PartNumber: partNumber,
                                    UploadId: uploadId,
                                    Body: chunk,
                                })
                            )
                        )
                );
                return uploadPartResponse.ETag!;
            } catch (error) {
                this.ctx.handleRetryErrorLogging(
                    attempt,
                    `to upload part ${partNumber} of ${filename}`,
                    error
                );

                // Wait before next attempt
                await wait(backoffDelay(attempt));
            }
        }
    }

    // ════════════════════════════════════════════════════════════════
    // 🔄 STREAM TO ITERABLE
    // Converts a readable stream into iterable buffer chunks
    // ════════════════════════════════════════════════════════════════

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
                while (buffer.length >= this.ctx.maxUploadPartSize) {
                    yield buffer.subarray(0, this.ctx.maxUploadPartSize);
                    buffer = buffer.subarray(this.ctx.maxUploadPartSize);
                }
            }
            if (buffer.length) yield buffer;
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
                while (buffer.length >= this.ctx.maxUploadPartSize) {
                    yield buffer.subarray(0, this.ctx.maxUploadPartSize);
                    buffer = buffer.subarray(this.ctx.maxUploadPartSize);
                }
            }
            if (buffer.length) yield buffer;
        }
    }

    // ════════════════════════════════════════════════════════════════
    // 📚 BUFFER TO CHUNKS
    // Splits a Buffer into multipart-sized chunks
    // ════════════════════════════════════════════════════════════════

    private bufferToChunks(buffer: Buffer): Buffer[] {
        const bufferChunks: Buffer[] = [];
        const totalFileSize = buffer.length;
        const numberOfParts = Math.ceil(
            totalFileSize / this.ctx.maxUploadPartSize
        );

        for (let part = 1; part <= numberOfParts; part++) {
            const start = (part - 1) * this.ctx.maxUploadPartSize;
            const end = Math.min(
                start + this.ctx.maxUploadPartSize,
                totalFileSize
            );
            const chunk = buffer.subarray(start, end);
            bufferChunks.push(chunk);
        }
        return bufferChunks;
    }
}
