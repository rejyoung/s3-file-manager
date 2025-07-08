import {
    CopyObjectCommand,
    DeleteObjectCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import { S3FMContext } from "./context.js";
import { backoffDelay, wait } from "../utils/wait.js";
import {
    ListDirectoriesOptions,
    ListFilesOptions,
    MoveFileOptions,
    RenameFileOptions,
} from "../types/input-types.js";
import path from "path";
import {
    ConfirmFilesOptionsInternal,
    CopyFileOptionsInternal,
    DeleteFileOptionsInternal,
    ListItemsOptionsInternal,
} from "../types/internal-types.js";
import {
    CopyReturnType,
    DeleteReturnType,
    MoveReturnType,
    RenameReturnType,
} from "../types/return-types.js";

/**
 ╔════════════════════════════════════════════════════════════════════════════════╗
 ║ 🧾 FILE SERVICE                                                                ║
 ║ Public-facing interface for loading, saving, or transferring file data         ║
 ║ through the S3 storage layer. Orchestrates upload/download logic.              ║
 ╚════════════════════════════════════════════════════════════════════════════════╝
 */
export class FileService {
    private ctx: S3FMContext;

    constructor(context: S3FMContext) {
        this.ctx = context;
    }

    // ════════════════════════════════════════════════════════════════
    // 📂 LIST ITEMS
    // General-purpose function for listing files or directories
    // ════════════════════════════════════════════════════════════════

    private async listItems(
        options: ListItemsOptionsInternal
    ): Promise<string[]> {
        const {
            prefix,
            filterFn = (fileName: string) => true,
            compareFn = undefined,
            directoriesOnly = false,
            spanOptions,
        } = options;

        const { name: spanName, attributes: spanAttributes } = spanOptions;

        const params = {
            Bucket: this.ctx.bucketName,
            Prefix: prefix ? this.formatPrefix("", prefix) : undefined,
            Delimiter: directoriesOnly ? "/" : undefined,
        };

        // Assert that spanName and spanAttributes exist, as they must be passed in by the two wrapper functions
        const result = await this.ctx.withSpan(
            spanName!,
            spanAttributes!,
            async () => {
                const filteredItems: string[] = [];

                let continuationToken: string | undefined = undefined;

                do {
                    let attempt = 0;
                    let response;
                    while (true) {
                        try {
                            attempt++;
                            const command = new ListObjectsV2Command(params);
                            response = await this.ctx.s3.send(command);
                            break;
                        } catch (error) {
                            this.ctx.handleRetryErrorLogging(
                                attempt,
                                `to fetch list of ${
                                    directoriesOnly ? "directories" : "files"
                                }`,
                                error
                            );

                            // Wait before next attempt
                            await wait(backoffDelay(attempt));
                        }
                    }
                    let items: string[];
                    if (directoriesOnly) {
                        items =
                            response.CommonPrefixes?.map((p) => p.Prefix || "")
                                .filter(Boolean)
                                .filter(filterFn) || [];
                    } else {
                        items =
                            response.Contents?.map((file) => file.Key || "")
                                .filter(Boolean)
                                .filter(filterFn) || [];
                    }

                    filteredItems.push(...items);
                    continuationToken = response.ContinuationToken;
                } while (continuationToken);

                const sortedItems = filteredItems.sort(compareFn);

                this.ctx.verboseLog(
                    `Successfully retrieved ${sortedItems.length} ${
                        directoriesOnly
                            ? `director${
                                  sortedItems.length === 1 ? "y" : "ies"
                              }`
                            : "file(s)"
                    }${params.Prefix ? ` with prefix '${params.Prefix}'` : ""}'`
                );

                return sortedItems;
            }
        );
        return result;
    }

    // ════════════════════════════════════════════════════════════════
    // 📁 LIST DIRECTORIES
    // Lists all directories (common prefixes) from a specified prefix
    // ════════════════════════════════════════════════════════════════
    public async listDirectories(options: ListDirectoriesOptions = {}) {
        const {
            prefix,
            filterFn = (fileName: string) => true,
            compareFn = undefined,
            spanOptions = {},
        } = options;

        const {
            name: spanName = "S3FileManager.listDirectories",
            attributes: spanAttributes = {
                bucket: this.ctx.bucketName,
                prefix: prefix ?? "",
            },
        } = spanOptions;

        return await this.listItems({
            prefix,
            filterFn,
            compareFn,
            directoriesOnly: true,
            spanOptions: { name: spanName, attributes: spanAttributes },
        });
    }

    // ════════════════════════════════════════════════════════════════
    // 📄 LIST FILES
    // Lists all files (excluding directories) from a specified prefix
    // ════════════════════════════════════════════════════════════════
    public async listFiles(options: ListFilesOptions = {}) {
        const {
            prefix,
            filterFn = (fileName: string) => true,
            compareFn = undefined,
            spanOptions = {},
        } = options;

        const {
            name: spanName = "S3FileManager.listFiles",
            attributes: spanAttributes = {
                bucket: this.ctx.bucketName,
                prefix: prefix ?? "",
            },
        } = spanOptions;

        return await this.listItems({
            prefix,
            filterFn,
            compareFn,
            spanOptions: { name: spanName, attributes: spanAttributes },
        });
    }

    // ════════════════════════════════════════════════════════════════
    // ✅ CONFIRM FILE EXISTENCE
    // Verifies the presence of specified files in the S3 bucket using HeadObject
    // ════════════════════════════════════════════════════════════════
    public async confirmFilesExist(
        filenames: string[],
        options: ConfirmFilesOptionsInternal = {}
    ): Promise<string[]> {
        const { prefix, spanOptions = {}, bucketName } = options;

        const missingFiles: string[] = [];

        await Promise.all(
            filenames.map(async (filename) => {
                const {
                    name: spanName = "S3FileManager.confirmFilesExist",
                    attributes: spanAttributes = {
                        bucket: bucketName ?? this.ctx.bucketName,
                        filename: `${prefix ?? ""}${filename}`,
                    },
                } = spanOptions;

                await this.ctx.withSpan(spanName, spanAttributes, async () => {
                    let attempt = 0;
                    let success = false;

                    while (!success) {
                        try {
                            attempt++;
                            const command = new HeadObjectCommand({
                                Bucket: bucketName ?? this.ctx.bucketName,
                                Key: `${prefix ?? ""}${filename}`,
                            });
                            await this.ctx.s3.send(command);

                            success = true;
                        } catch (error: any) {
                            if (
                                error.name === "NotFound" ||
                                error.$metadata?.httpStatusCode === 404
                            ) {
                                this.ctx.verboseLog(
                                    `${filename} not found in bucket ${
                                        bucketName ?? this.ctx.bucketName
                                    }.`
                                );
                                missingFiles.push(filename);
                                success = true;
                            } else {
                                this.ctx.handleRetryErrorLogging(
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

        this.ctx.verboseLog(
            `Checked ${filenames.length} file(s); missing: ${missingFiles.length}`
        );

        return missingFiles;
    }

    // ════════════════════════════════════════════════════════════════
    // 📄 COPY FILE
    // Copies a file from one location to another, possibly renaming it.
    // Handles source bucket override and retry logic.
    // ════════════════════════════════════════════════════════════════
    public async copyFile(
        filePath: string,
        destinationPrefix: string,
        options: CopyFileOptionsInternal = {}
    ): Promise<CopyReturnType> {
        const { spanOptions = {}, sourceBucketName, newFilename } = options;

        const {
            name: spanName = "S3FileManager.copyFile",
            attributes: spanAttributes = {
                sourceBucket: sourceBucketName ?? this.ctx.bucketName,
                bucket: this.ctx.bucketName,
                filePath,
                destinationPrefix,
            },
        } = spanOptions;

        const trimmedFilename = path.posix.basename(filePath).trim();

        const toPrefix = destinationPrefix
            ? this.formatPrefix(trimmedFilename, destinationPrefix.trim())
            : "";

        const sourcePath = `${
            sourceBucketName ?? this.ctx.bucketName
        }/${filePath}`;

        // Form destination path, subbing in new filename if provided
        const destinationPath = `${toPrefix}${
            newFilename?.trim() ?? trimmedFilename
        }`;

        const result = await this.ctx.withSpan(
            spanName,
            spanAttributes,
            async () => {
                let attempt = 0;

                while (true) {
                    try {
                        await this.ctx.s3.send(
                            new CopyObjectCommand({
                                Bucket: this.ctx.bucketName,
                                CopySource: sourcePath,
                                Key: destinationPath,
                            })
                        );
                        return {
                            success: true,
                            source: sourcePath,
                            destination: destinationPath,
                        };
                    } catch (error) {
                        this.ctx.handleRetryErrorLogging(
                            attempt,
                            `to copy ${trimmedFilename}`,
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
    // 🔀 MOVE FILE
    // Copies the file to the new prefix, then deletes the original.
    // Handles source bucket override and retry logic.
    // ════════════════════════════════════════════════════════════════
    public async moveFile(
        filePath: string,
        destinationPrefix: string,
        options: MoveFileOptions = {}
    ): Promise<MoveReturnType> {
        const { spanOptions = {}, sourceBucketName } = options;

        const {
            name: spanName = "S3FileManager.moveFile",
            attributes: spanAttributes = {
                sourceBucket: sourceBucketName ?? this.ctx.bucketName,
                bucket: this.ctx.bucketName,
                filePath,
                destinationPrefix,
            },
        } = spanOptions;

        const result = await this.ctx.withSpan(
            spanName,
            spanAttributes,
            async () => {
                try {
                    await this.copyFile(filePath, destinationPrefix, {
                        ...options,
                        spanOptions: {
                            name: `S3FileManager.moveFile > copyFile`,
                            attributes: spanAttributes,
                        },
                    });
                    this.ctx.verboseLog(
                        `File ${filePath} successfully copied to ${destinationPrefix}.`
                    );

                    const { success: deleteSuccess } = await this.deleteFile(
                        filePath,
                        {
                            ...options,
                            sourceBucketName,
                            spanOptions: {
                                name: `S3FileManager.moveFile > deleteFile`,
                                attributes: {
                                    filePath,
                                    bucket:
                                        sourceBucketName ?? this.ctx.bucketName,
                                },
                            },
                        }
                    );

                    const trimmedFilename = path.posix
                        .basename(filePath)
                        .trim();
                    this.ctx.verboseLog(
                        `File ${trimmedFilename} successfully deleted from original location.`
                    );
                    this.ctx.verboseLog(
                        `File ${trimmedFilename} successfully moved to ${this.ctx.bucketName}/${destinationPrefix}.`
                    );

                    return {
                        success: true,
                        source: filePath,
                        destination: destinationPrefix,
                        originalDeleted: deleteSuccess,
                    };
                } catch (error) {
                    throw error;
                }
            }
        );
        return result;
    }

    // ════════════════════════════════════════════════════════════════
    // ✏️ RENAME FILE
    // Renames a file within the same location by copying and deleting the original.
    // ════════════════════════════════════════════════════════════════
    public async renameFile(
        filePath: string,
        newFilename: string,
        options: RenameFileOptions = {}
    ): Promise<RenameReturnType> {
        const { spanOptions = {} } = options;

        const {
            name: spanName = "S3FileManager.renameFile",
            attributes: spanAttributes = {
                bucket: this.ctx.bucketName,
                filePath: filePath,
                newFilename,
            },
        } = spanOptions;

        const result = await this.ctx.withSpan(
            spanName,
            spanAttributes,
            async () => {
                const location = path.posix.dirname(filePath);

                await this.copyFile(filePath, location, {
                    newFilename,
                    spanOptions: {
                        name: "S3FileManager.renameFile > copyFile",
                        attributes: {
                            bucket: this.ctx.bucketName,
                            filePath,
                            newFilename,
                        },
                    },
                });
                this.ctx.verboseLog(
                    `Successfully copied file ${filePath} to same location with new name ${newFilename}.`
                );

                const { success: deleteSuccess } = await this.deleteFile(
                    filePath,
                    {
                        spanOptions: {
                            name: "S3FileManager.renameFile > deleteFile",
                            attributes: { oldFile: filePath },
                        },
                    }
                );
                this.ctx.verboseLog(
                    `Successfully deleted original copy of ${filePath} with old name.`
                );

                return {
                    success: true,
                    oldPath: filePath,
                    newPath: location + newFilename,
                    originalDeleted: deleteSuccess,
                };
            }
        );
        return result;
    }

    // ════════════════════════════════════════════════════════════════
    // 🗑 DELETE FILE FROM S3
    // Deletes a file and handles NoSuchKey gracefully
    // ════════════════════════════════════════════════════════════════
    public async deleteFile(
        filePath: string,
        options: DeleteFileOptionsInternal = {}
    ): Promise<DeleteReturnType> {
        const { spanOptions = {}, sourceBucketName } = options;

        const {
            name: spanName = "S3FileManager.deleteFile",
            attributes: spanAttributes = {
                bucket: sourceBucketName ?? this.ctx.bucketName,
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
                        const command = new DeleteObjectCommand({
                            Bucket: sourceBucketName ?? this.ctx.bucketName,
                            Key: filePath,
                        });

                        await this.ctx.s3.send(command);

                        this.ctx.logger.info(
                            `Successfully deleted file: ${
                                sourceBucketName ? sourceBucketName + "/" : ""
                            }${filePath}`
                        );
                        return {
                            success: true,
                            deleted: true,
                            filePath: filePath,
                        };
                    } catch (error: any) {
                        if (error.name === "NoSuchKey") {
                            this.ctx.logger.warn(
                                `File ${
                                    sourceBucketName
                                        ? sourceBucketName + "/"
                                        : ""
                                }${filePath} not found, nothing to delete`
                            );
                            return {
                                success: false,
                                deleted: false,
                                filePath,
                                reason: "File not found",
                            };
                        }

                        this.ctx.handleRetryErrorLogging(
                            attempt,
                            `to delete file ${
                                sourceBucketName ? sourceBucketName + "/" : ""
                            }${filePath}`,
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
    //
    // ════════════════════════════════════════════════════════════════

    public deleteFolder() {}

    private formatPrefix(filename: string, prefix: string): string {
        let formattedPrefix: string;

        const prefixSlash = prefix.endsWith("/");
        const filenameSlash = filename.startsWith("/");

        if (prefixSlash && filenameSlash) {
            formattedPrefix = prefix.slice(0, -1);
        } else if (!prefixSlash && !filenameSlash) {
            formattedPrefix = prefix + "/";
        } else {
            formattedPrefix = prefix;
        }

        return formattedPrefix;
    }
}
