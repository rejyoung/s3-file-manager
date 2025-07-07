import { GetObjectCommand } from "@aws-sdk/client-s3";
import { S3FMContext } from "./context.js";
import { Readable } from "stream";
import { backoffDelay, wait } from "../utils/wait.js";
import { FileStreamOptions } from "../types/input-types.js";

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
        options: FileStreamOptions
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
}
