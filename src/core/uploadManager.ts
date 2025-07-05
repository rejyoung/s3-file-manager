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
import {
    FilePayload,
    FileUploadOptions,
    Stream,
} from "../types/input-types.js";
import { UploadContentType } from "../types/internal-types.js";
import { wait } from "../utils/wait.js";
import { backoffDelay } from "../utils/wait.js";
import { isStreamType, StreamType } from "../utils/type-guards.js";
import { lookup as mimeLookup } from "mime-types";

export class UploadManager {
    private shared: S3FMContext;

    constructor(context: S3FMContext) {
        this.shared = context;
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

            this.shared.verboseLog(
                `Uploading ${file.name} (${sizeInBytes} bytes) using ${
                    sizeInBytes && sizeInBytes > this.shared.multipartThreshold
                        ? "multipart"
                        : "simple"
                } upload`
            );

            const mimeType =
                options.contentType ??
                (mimeLookup(file.name) || "application/octet-stream");

            if (sizeInBytes && sizeInBytes > this.shared.multipartThreshold) {
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
                bucket: this.shared.bucketName,
                filename: `${prefix ?? ""}${file.name}`,
            },
        } = spanOptions;

        const key = `${prefix ?? ""}${file.name}`;
        let attempt = 0;

        await this.shared.withSpan(spanName, spanAttributes, async () => {
            while (true) {
                try {
                    attempt++;
                    const command = new PutObjectCommand({
                        Bucket: this.shared.bucketName,
                        Key: key,
                        Body: file.content,
                        ContentType: mimeType,
                    });
                    await this.shared.s3.send(command);
                    this.shared.verboseLog(
                        `Successfully uploaded ${file.name}`
                    );
                    return;
                } catch (error) {
                    this.shared.handleRetryErrorLogging(
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
                bucket: this.shared.bucketName,
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
                this.shared.verboseLog(
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
                this.shared.verboseLog(
                    "Successfully constructed Buffer for multipart upload."
                );
            }
        } catch (error) {
            throw new Error(
                `Something went wrong while attempting to prepare content for multipart upload: ${this.shared.errorString(
                    error
                )}`,
                { cause: error }
            );
        }

        const response = await this.shared.withSpan(
            spanName,
            spanAttributes,
            async () => {
                let uploadId: string | undefined;
                const filename = `${prefix ?? ""}${file.name}`;

                let attempt = 0;
                while (true) {
                    attempt++;
                    try {
                        const createResponse = await this.shared.s3.send(
                            new CreateMultipartUploadCommand({
                                Bucket: this.shared.bucketName,
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

                        await this.shared.s3.send(
                            new CompleteMultipartUploadCommand({
                                Bucket: this.shared.bucketName,
                                Key: filename,
                                UploadId: uploadId,
                                MultipartUpload: { Parts: parts },
                            })
                        );
                        this.shared.verboseLog(
                            `File ${filename} successfully uploaded to S3 bucket`
                        );
                        return true;
                    } catch (error) {
                        await this.shared.s3.send(
                            new AbortMultipartUploadCommand({
                                Bucket: this.shared.bucketName,
                                Key: filename,
                                UploadId: uploadId,
                            })
                        );

                        this.shared.handleRetryErrorLogging(
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
                const uploadPartResponse = await this.shared.s3.send(
                    new UploadPartCommand({
                        Bucket: this.shared.bucketName,
                        Key: filename,
                        PartNumber: partNumber,
                        UploadId: uploadId,
                        Body: chunk,
                    })
                );
                return uploadPartResponse.ETag!;
            } catch (error) {
                this.shared.handleRetryErrorLogging(
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
                while (buffer.length >= this.shared.multipartThreshold) {
                    yield buffer.subarray(0, this.shared.multipartThreshold);
                    buffer = buffer.subarray(this.shared.multipartThreshold);
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
                while (buffer.length >= this.shared.multipartThreshold) {
                    yield buffer.subarray(0, this.shared.multipartThreshold);
                    buffer = buffer.subarray(this.shared.multipartThreshold);
                }
            }
            if (buffer.length) yield buffer;
        }
    }

    private bufferToChunks(buffer: Buffer): Array<Buffer> {
        const bufferChunks: Array<Buffer> = [];
        const totalFileSize = buffer.length;
        const numberOfParts = Math.ceil(
            totalFileSize / this.shared.multipartThreshold
        );

        for (let part = 1; part <= numberOfParts; part++) {
            const start = (part - 1) * this.shared.multipartThreshold;
            const end = Math.min(
                start + this.shared.multipartThreshold,
                totalFileSize
            );
            const chunk = buffer.subarray(start, end);
            bufferChunks.push(chunk);
        }
        return bufferChunks;
    }
}
