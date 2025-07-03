import {
    HeadObjectCommand,
    ListObjectsV2Command,
    S3Client,
} from "@aws-sdk/client-s3";
import { FMConfig, Logger, WithSpanFn } from "./types/fmconfig-types.js";
import { isValidLogger } from "./utils/isValidLogger.js";
import { ConfirmFilesOptions, ListFilesOptions } from "./types/input-types.js";

export class S3FileManager {
    private bucketName: string;
    private s3: S3Client;
    private logger: Logger;
    private withSpan: WithSpanFn;
    private maxAttempts: number;
    private attemptNumString: string;

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

        this.maxAttempts = config.maxAttempts ?? 3;
        this.attemptNumString = `${this.maxAttempts} attempt${
            this.maxAttempts > 1 ? "s" : ""
        }`;
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

        let attempt = 0;
        while (true) {
            try {
                attempt++;
                const command = new ListObjectsV2Command(params);
                const response = await this.withSpan(
                    spanName,
                    spanAttributes,
                    async () => await this.s3.send(command)
                );

                const files =
                    response.Contents?.map((file) => file.Key || "")
                        .filter(Boolean)
                        .filter(filterFn) || [];

                const sortedFiles = files?.sort(compareFn);

                return sortedFiles;
            } catch (error) {
                if (attempt === this.maxAttempts) {
                    throw new Error(
                        `Could not fetch list of files after ${
                            this.attemptNumString
                        }: ${this.errorString(error)}`,
                        { cause: error }
                    );
                }

                this.logger.warn(
                    `Attempt ${attempt} to fetch list of files failed. Retrying...`
                );
            }
        }
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

                let attempt = 0;
                let success = false;
                while (!success) {
                    try {
                        attempt++;
                        const command = new HeadObjectCommand({
                            Bucket: this.bucketName,
                            Key: `${prefix ?? ""}${filename}`,
                        });
                        await this.withSpan(
                            spanName,
                            spanAttributes,
                            async () => await this.s3.send(command)
                        );
                        success = true;
                    } catch (error: any) {
                        if (
                            error.name === "NotFound" ||
                            error.$metadata?.httpStatusCode === 404
                        ) {
                            missingFiles.push(filename);
                            success = true;
                        } else {
                            if (attempt === this.maxAttempts) {
                                throw new Error(
                                    `Attempt to verify the existence of file ${filename} failed after ${
                                        this.attemptNumString
                                    } ${this.errorString(error)}`,
                                    { cause: error }
                                );
                            }
                            this.logger.warn(
                                `Attempt ${attempt} to verify the existence of file ${filename} failed: ${this.errorString(
                                    error
                                )}`
                            );
                            this.logger.warn("Retrying...");
                        }
                    }
                }
            })
        );

        return {
            allExist: missingFiles.length === 0,
            missingFiles,
        };
    }

    private errorString(err: any) {
        return err instanceof Error ? err.message : String(err);
    }
}
