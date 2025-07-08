import {
    GetObjectCommand,
    HeadObjectCommand,
    HeadObjectCommandOutput,
} from "@aws-sdk/client-s3";
import { S3FMContext } from "./context.js";
import { Readable } from "stream";
import { backoffDelay, wait } from "../utils/wait.js";
import {
    DownloadAllOptions,
    DownloadLinkOptions,
    DownloadToDiskOptions,
    FileStreamOptions,
    LoadFileOptions,
} from "../types/input-types.js";
import { fileTypeFromBuffer } from "file-type";
import isUtf8 from "is-utf8";
import { createWriteStream } from "fs";
import { pipeline } from "stream/promises";
import { writeFile } from "fs/promises";
import path from "path";
import {
    FileContentType,
    FileMetadata,
    GetFileFormatInput,
} from "../types/internal-types.js";
import { extension } from "mime-types";

const TEXT_MIME_PREFIXES = ["text/", "application/xml"];
const TEXT_EXTENSIONS = ["txt", "csv", "xml", "md", "html"];

/**
 ╔════════════════════════════════════════════════════════════════════════════════╗
 ║ 📥 DOWNLOAD MANAGER                                                            ║
 ║ Manages downloads from S3, supporting buffered and streamed file retrieval,    ║
 ║ with support for metadata extraction and type detection.                       ║
 ╚════════════════════════════════════════════════════════════════════════════════╝
 */
export class DownloadManager {
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
            name: spanName = "S3FileManager.loadFile",
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

                        const stream: Readable = await this.streamFile(
                            filePath
                        );
                        const fileBuffer: Buffer =
                            await this.ctx.streamToBuffer(stream, "Readable");

                        const fileFormat = await this.getFileFormat({
                            filePath,
                            callerName: "S3FileManager.loadFile",
                        });
                        const returnType = fileFormat.fileType;

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
                            `to load file: ${filePath}`,
                            error
                        );

                        await wait(backoffDelay(attempt));
                    }
                }
            }
        );
        return result;
    }

    // ════════════════════════════════════════════════════════════════
    // 💾 DOWNLOAD TO DISK
    // Downloads a file from S3 and writes it to the local file system
    // ════════════════════════════════════════════════════════════════
    public async downloadToDisk(
        filePath: string,
        outDir: string,
        options: DownloadToDiskOptions
    ): Promise<void> {
        const { spanOptions = {}, outputFilename } = options;

        const {
            name: spanName = "S3FileManager.downloadToDisk",
            attributes: spanAttributes = {
                bucket: this.ctx.bucketName,
                filePath: filePath,
                outDir: outDir,
            },
        } = spanOptions;

        // Normalize and correctly format outDir
        const normalizedOutDir = path.normalize(outDir);
        const formattedOutDir = path.join(normalizedOutDir, "");

        await this.ctx.withSpan(spanName, spanAttributes, async () => {
            const fileMetadata = await this.getFileMetadata(
                filePath,
                "S3FileManager.downloadToDisk"
            );
            const originalFileName = path.parse(filePath).name;

            const stream: Readable = await this.streamFile(filePath);

            let fileBuffer: Buffer | undefined;
            if (
                fileMetadata.contentLength &&
                fileMetadata.contentLength <= 200 * 1024 * 1024
            ) {
                fileBuffer = await this.ctx.streamToBuffer(stream, "Readable");
            }

            let destinationPath: string;
            if (outputFilename) {
                destinationPath = formattedOutDir + outputFilename;
            } else {
                const fileFormat = await this.getFileFormat({
                    filePath,
                    callerName: "S3FileManager.downloadToDisk",
                    mimeType: fileMetadata.mimeType,
                    fileBuffer,
                });
                destinationPath = formattedOutDir + originalFileName;

                if (fileFormat.extension) {
                    destinationPath += "." + fileFormat.extension;
                }
            }

            if (fileBuffer) {
                await writeFile(destinationPath, fileBuffer);
            } else {
                await pipeline(stream, createWriteStream(destinationPath));
            }
        });
    }

    // ════════════════════════════════════════════════════════════════
    //
    //
    // ════════════════════════════════════════════════════════════════

    public downloadAllToDisk(prefix: string, options: DownloadAllOptions) {}

    // ════════════════════════════════════════════════════════════════
    //
    //
    // ════════════════════════════════════════════════════════════════
    public generateTempDownloadLink(
        filePath: string,
        options: DownloadLinkOptions
    ) {}

    // ════════════════════════════════════════════════════════════════
    // 🧾 GET FILE METADATA
    // Retrieves file metadata such as MIME type and content length
    // ════════════════════════════════════════════════════════════════
    private async getFileMetadata(
        filePath: string,
        callerName: string
    ): Promise<FileMetadata> {
        const command = new HeadObjectCommand({
            Bucket: this.ctx.bucketName,
            Key: filePath,
        });
        const s3MetaData = await this.ctx.withSpan<HeadObjectCommandOutput>(
            `${callerName} > getMimeType`,
            { filePath },
            async () => {
                let attempt = 0;
                while (true) {
                    try {
                        const response = await this.ctx.s3.send(command);

                        return response;
                    } catch (error: any) {
                        if (
                            error.name === "NotFound" ||
                            error.$metadata?.httpStatusCode === 404
                        ) {
                            throw new Error(`File ${filePath} not found`);
                        }
                        this.ctx.handleRetryErrorLogging(
                            attempt,
                            `to get MIME type of file ${filePath}`,
                            error
                        );
                    }
                }
            }
        );

        if (!s3MetaData.ContentType)
            this.ctx.logger.warn(`Missing ContentType for ${filePath}`);

        const fileMetadata = {
            mimeType: s3MetaData.ContentType,
            contentLength: s3MetaData.ContentLength,
        };

        return fileMetadata;
    }

    // ════════════════════════════════════════════════════════════════
    // 🧪 DETERMINE FILE FORMAT
    // Determines file content type and best-guess extension
    // ════════════════════════════════════════════════════════════════
    private async getFileFormat({
        filePath,
        callerName,
        fileBuffer,
        mimeType,
    }: GetFileFormatInput): Promise<{
        fileType: FileContentType;
        extension: string;
    }> {
        const filePathLC = filePath.toLowerCase();

        if (!mimeType) {
            mimeType = (
                await this.getFileMetadata(
                    filePath,
                    `${callerName} > getFileFormat`
                )
            ).mimeType;
        }

        const fileType = fileBuffer
            ? await fileTypeFromBuffer(fileBuffer)
            : undefined;

        // Determine file content type
        let returnType: FileContentType = "buffer";

        if (mimeType && mimeType !== "application/octet-stream") {
            if (mimeType === "application/json") {
                returnType = "json";
            } else if (
                TEXT_MIME_PREFIXES.some((prefix) => mimeType.startsWith(prefix))
            ) {
                returnType = "text";
            }
        } else if (fileType || (fileBuffer && !isUtf8(fileBuffer))) {
            returnType = "buffer";
        } else if (filePathLC.endsWith("json")) {
            returnType = "json";
        } else if (
            TEXT_EXTENSIONS.some((extension) => filePathLC.endsWith(extension))
        ) {
            returnType = "text";
        } else {
            returnType = "buffer";
        }

        // Get file extension
        let ext =
            extension(mimeType || "") ||
            fileType?.ext ||
            path.extname(filePath).slice(1);

        if (ext === "")
            this.ctx.logger.warn(
                `Unable to determine a file extension for file ${filePath}`
            );

        return { fileType: returnType, extension: ext };
    }
}
