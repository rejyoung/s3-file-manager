import { HeadObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { S3FMContext } from "./context.js";
import { backoffDelay, wait } from "../utils/wait.js";
import { ConfirmFilesOptions, ListFilesOptions } from "../types/input-types.js";

export class FileService {
    private shared: S3FMContext;

    constructor(context: S3FMContext) {
        this.shared = context;
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
                bucket: this.shared.bucketName,
                prefix: prefix ?? "",
            },
        } = spanOptions;

        const params = {
            Bucket: this.shared.bucketName,
            Prefix: prefix,
        };

        const result = await this.shared.withSpan(
            spanName,
            spanAttributes,
            async () => {
                let attempt = 0;

                while (true) {
                    try {
                        attempt++;
                        const command = new ListObjectsV2Command(params);
                        const response = await this.shared.s3.send(command);

                        const files =
                            response.Contents?.map((file) => file.Key || "")
                                .filter(Boolean)
                                .filter(filterFn) || [];

                        const sortedFiles = files?.sort(compareFn);

                        this.shared.verboseLog(
                            `Successfully retrieved ${files.length} file(s)${
                                params.Prefix
                                    ? ` with prefix '${params.Prefix}'`
                                    : ""
                            }'`
                        );

                        return sortedFiles;
                    } catch (error) {
                        this.shared.handleRetryErrorLogging(
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
                        bucket: this.shared.bucketName,
                        filename: `${prefix ?? ""}${filename}`,
                    },
                } = spanOptions;

                await this.shared.withSpan(
                    spanName,
                    spanAttributes,
                    async () => {
                        let attempt = 0;
                        let success = false;

                        while (!success) {
                            try {
                                attempt++;
                                const command = new HeadObjectCommand({
                                    Bucket: this.shared.bucketName,
                                    Key: `${prefix ?? ""}${filename}`,
                                });
                                await this.shared.s3.send(command);

                                success = true;
                            } catch (error: any) {
                                if (
                                    error.name === "NotFound" ||
                                    error.$metadata?.httpStatusCode === 404
                                ) {
                                    this.shared.verboseLog(
                                        `${filename} not found in bucket ${this.shared.bucketName}.`
                                    );
                                    missingFiles.push(filename);
                                    success = true;
                                } else {
                                    this.shared.handleRetryErrorLogging(
                                        attempt,
                                        `to verify the existence of file ${filename}`,
                                        error
                                    );
                                    // Wait before next attempt
                                    await wait(backoffDelay(attempt));
                                }
                            }
                        }
                    }
                );
            })
        );

        const result = {
            allExist: missingFiles.length === 0,
            missingFiles,
        };

        this.shared.verboseLog(
            `Checked ${filenames.length} file(s); missing: ${missingFiles.length}`
        );

        return result;
    }
}
