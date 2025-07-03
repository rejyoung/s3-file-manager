import { ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import { FMConfig, Logger, WithSpanFn } from "./types/fmconfig-types.js";
import { isValidLogger } from "./utils/isValidLogger.js";
import { ListFilesOptions } from "./types/input-types.js";

export class S3FileManager {
    private bucketName: string;
    private s3: S3Client;
    private logger: Logger;
    private withSpan: WithSpanFn;
    private maxAttempts: number;

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
                        `Could not fetch list of files after ${attempt} attempt${
                            this.maxAttempts > 1 ? "s" : ""
                        }: ${
                            error instanceof Error
                                ? error.message
                                : String(error)
                        }`
                    );
                }

                this.logger.warn(
                    `Attempt ${attempt} to fetch list of files failed. Retrying...`
                );
            }
        }
    }
}
