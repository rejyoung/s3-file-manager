import { GetObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import { S3FMContext } from "./context.js";
import { Readable } from "stream";
import { backoffDelay, wait } from "../utils/wait.js";
import { FileStreamOptions, LoadFileOptions } from "../types/input-types.js";
import { fileTypeFromBuffer } from "file-type";
import isUtf8 from "is-utf8";

const TEXT_MIME_PREFIXES = ["text/", "application/xml"];
const TEXT_EXTENSIONS = ["txt", "csv", "xml", "md", "html"];

export class downloadManager {
    private ctx: S3FMContext;
    constructor(context: S3FMContext) {
        this.ctx = context;
    }

    // ════════════════════════════════════════════════════════════════
    // 🚿 STREAM FILE FROM S3
    // Streams file data without loading it fully into memory
    // ════════════════════════════════════════════════════════════════
    public async streamFile(
        filePath: string,
        options: FileStreamOptions = {}
    ): Promise<Readable> {
        const { spanOptions = {}, timeoutMS = 10000 } = options;

        const {
            name: spanName = "S3FileManager.streamFile",
            attributes: spanAttributes = {
                bucket: this.ctx.bucketName,
                filePath: filePath,
            },
        } = spanOptions;

        return await this.ctx.withSpan(spanName, spanAttributes, async () => {
            let attempt = 0;
            while (true) {
                // Set up timeout function
                const controller = new AbortController();
                const timeout = setTimeout(() => controller.abort(), timeoutMS);

                let response: any;
                try {
                    attempt++;
                    const command = new GetObjectCommand({
                        Bucket: this.ctx.bucketName,
                        Key: filePath,
                    });

                    response = await this.ctx.s3.send(command, {
                        abortSignal: controller.signal,
                    });

                    clearTimeout(timeout);

                    if (!response.Body) {
                        throw new Error(
                            `File ${filePath} not found in bucket ${this.ctx.bucketName}`
                        );
                    }

                    this.ctx.verboseLog(`Streaming file: ${filePath}`);
                    return response.Body as Readable;
                } catch (error: any) {
                    clearTimeout(timeout);

                    // Close the stream if still open in case of error
                    if (
                        response?.Body &&
                        "readableEnded" in response.Body &&
                        !response.Body.readableEnded
                    ) {
                        (response.Body as Readable).destroy();
                    }

                    if (error.name === "AbortError") {
                        this.ctx.logger.warn(
                            `Streaming ${filePath} timed out after ${timeoutMS}ms`
                        );
                    }

                    this.ctx.handleRetryErrorLogging(
                        attempt,
                        `to stream ${filePath}`,
                        error
                    );

                    await wait(backoffDelay(attempt));
                }
            }
        });
    }

    // ════════════════════════════════════════════════════════════════
    // 📄 LOAD FILE CONTENTS
    // Loads a file's contents into memory as Buffer, text, or object
    // ════════════════════════════════════════════════════════════════
    public async loadFile(
        filePath: string,
        options: LoadFileOptions = {}
    ): Promise<string | Buffer | object> {
        const { spanOptions = {} } = options;

        const {
            name: spanName = "S3FileManager.downloadFile",
            attributes: spanAttributes = {
                bucket: this.ctx.bucketName,
                filePath: filePath,
            },
        } = spanOptions;

        const result = await this.ctx.withSpan(
            spanName,
            spanAttributes,
            async () => {
                let attempt = 0;
                while (true) {
                    try {
                        attempt++;
                        const fileBuffer: Buffer = await this.ctx.withSpan(
                            spanName,
                            spanAttributes,
                            async () => {
                                const stream = await this.streamFile(filePath);
                                return await this.ctx.streamToBuffer(
                                    stream,
                                    "Readable"
                                );
                            }
                        );

                        let returnType = "buffer";
                        const mimeType = await this.getMimeType(filePath);
                        const fileType = await fileTypeFromBuffer(fileBuffer);

                        if (
                            mimeType &&
                            mimeType !== "application/octet-stream"
                        ) {
                            if (mimeType === "application/json") {
                                returnType = "json";
                            } else if (
                                TEXT_MIME_PREFIXES.some((prefix) =>
                                    mimeType.startsWith(prefix)
                                )
                            ) {
                                returnType = "text";
                            }
                        } else if (fileType || !isUtf8(fileBuffer)) {
                            returnType = "buffer";
                        } else if (filePath.toLowerCase().endsWith("json")) {
                            returnType = "json";
                        } else if (
                            TEXT_EXTENSIONS.some((extension) =>
                                filePath.toLowerCase().endsWith(extension)
                            )
                        ) {
                            returnType = "text";
                        } else {
                            returnType = "buffer";
                        }

                        switch (returnType) {
                            case "text":
                                return fileBuffer.toString("utf-8");
                            case "json":
                                return JSON.parse(fileBuffer.toString("utf-8"));
                            default:
                                return fileBuffer;
                        }
                    } catch (error) {
                        this.ctx.handleRetryErrorLogging(
                            attempt,
                            `to download file: ${filePath}`,
                            error
                        );

                        await wait(backoffDelay(attempt));
                    }
                }
            }
        );
        return result;
    }

    private async getMimeType(filePath: string): Promise<string | undefined> {
        const command = new HeadObjectCommand({
            Bucket: this.ctx.bucketName,
            Key: filePath,
        });
        const response = await this.ctx.s3.send(command);
        return response.ContentType;
    }
}
