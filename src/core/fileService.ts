import {
    CopyObjectCommand,
    DeleteObjectCommand,
    DeleteObjectsCommand,
    HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { S3FMContext } from "./context.js";
import { backoffDelay, wait } from "../utils/wait.js";
import {
    DeleteFolderOptions,
    ListFoldersOptions,
    ListFilesOptions,
    MoveFileOptions,
    RenameFileOptions,
} from "../types/input-types.js";
import path from "path";
import {
    VerifyFilesOptionsInternal,
    CopyFileOptionsInternal,
    DeleteFileOptionsInternal,
} from "../types/internal-types.js";
import {
    CopyReturnType,
    DeleteFolderReturnType,
    FileDeletionError,
    MoveReturnType,
    RenameReturnType,
} from "../types/return-types.js";
import { formatPrefix } from "../utils/formatPrefix.js";

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
    // 📁 LIST DIRECTORIES
    // Lists all directories (common prefixes) from a specified prefix
    // ════════════════════════════════════════════════════════════════
    public async listFolders(
        prefix: string,
        options: ListFoldersOptions = {}
    ): Promise<string[]> {
        const {
            filterFn = (fileName: string) => true,
            compareFn = undefined,
            spanOptions = {},
        } = options;

        const {
            name: spanName = "S3FileManager.listFolders",
            attributes: spanAttributes = {
                bucket: this.ctx.bucketName,
                prefix: prefix ?? "",
            },
        } = spanOptions;

        return await this.ctx.listItems(prefix, {
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
    public async listFiles(
        prefix: string,
        options: ListFilesOptions = {}
    ): Promise<string[]> {
        const {
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

        return await this.ctx.listItems(prefix, {
            filterFn,
            compareFn,
            spanOptions: { name: spanName, attributes: spanAttributes },
        });
    }

    // ════════════════════════════════════════════════════════════════
    // ✅ CONFIRM FILE EXISTENCE
    // Verifies the presence of specified files in the S3 bucket using HeadObject
    // ════════════════════════════════════════════════════════════════
    public async verifyFilesExist(
        filenames: string[],
        options: VerifyFilesOptionsInternal = {}
    ): Promise<string[]> {
        const { prefix, spanOptions = {}, bucketName } = options;

        const missingFiles: string[] = [];

        this.ctx.verboseLog(
            `Verifying existence of ${filenames.length} file(s)...`,
            "info"
        );
        await Promise.all(
            filenames.map(async (filename) => {
                const {
                    name: spanName = "S3FileManager.verifyFilesExist",
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
            ? formatPrefix(trimmedFilename, destinationPrefix.trim())
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
                        attempt++;
                        this.ctx.verboseLog(
                            `Attempting to copy from ${sourcePath} to ${destinationPath}`,
                            "info"
                        );
                        await this.ctx.s3.send(
                            new CopyObjectCommand({
                                Bucket: this.ctx.bucketName,
                                CopySource: sourcePath,
                                Key: destinationPath,
                            })
                        );
                        this.ctx.verboseLog(
                            `Successfully copied ${sourcePath} to ${destinationPath}`,
                            "info"
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

        this.ctx.verboseLog(
            `Starting move operation from ${filePath} to ${destinationPrefix}`,
            "info"
        );

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

                    let deleteSuccess = true;
                    try {
                        await this.deleteFile(filePath, {
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
                        });
                    } catch (error) {
                        deleteSuccess = false;
                    }

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

        this.ctx.verboseLog(`Renaming ${filePath} to ${newFilename}`, "info");

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

                let deleteSuccess = true;
                try {
                    await this.deleteFile(filePath, {
                        spanOptions: {
                            name: "S3FileManager.renameFile > deleteFile",
                            attributes: { oldFile: filePath },
                        },
                    });
                    this.ctx.verboseLog(
                        `Successfully deleted original copy of ${filePath} with old name.`
                    );
                } catch (error) {
                    deleteSuccess = false;
                    this.ctx.verboseLog(
                        `Unable to delete original copy of ${filePath} with old name.`,
                        "warn"
                    );
                }

                return {
                    success: true,
                    oldPath: filePath,
                    newPath: `${location}/${newFilename}`,
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
    ): Promise<void> {
        const { spanOptions = {}, sourceBucketName } = options;

        const {
            name: spanName = "S3FileManager.deleteFile",
            attributes: spanAttributes = {
                bucket: sourceBucketName ?? this.ctx.bucketName,
                filePath: filePath,
            },
        } = spanOptions;

        await this.ctx.withSpan(spanName, spanAttributes, async () => {
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
                    break;
                } catch (error: any) {
                    if (error.name === "NoSuchKey") {
                        this.ctx.verboseLog(
                            `File ${
                                sourceBucketName ? sourceBucketName + "/" : ""
                            }${filePath} not found, nothing to delete`,
                            "warn"
                        );
                        return;
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
        });
    }

    // ════════════════════════════════════════════════════════════════
    // 📂❌ DELETE FOLDER
    // Deletes all objects under the given prefix ("folder") in the S3 bucket
    // ════════════════════════════════════════════════════════════════

    public async deleteFolder(
        prefix: string,
        options: DeleteFolderOptions = {}
    ): Promise<DeleteFolderReturnType> {
        const { spanOptions = {} } = options;

        const {
            name: spanName = "S3FileManager.deleteFolder",
            attributes: spanAttributes = {
                bucket: this.ctx.bucketName,
                prefix: prefix,
            },
        } = spanOptions;

        const result = await this.ctx.withSpan(
            spanName,
            spanAttributes,
            async () => {
                try {
                    this.ctx.verboseLog(
                        `Gathering files to delete from folder: ${prefix}`,
                        "info"
                    );
                    const filePaths = await this.listFiles(prefix, {
                        spanOptions: {
                            name: "S3FileManager.deleteFolder > listFiles",
                            attributes: spanAttributes,
                        },
                    });

                    if (filePaths.length === 0) {
                        return {
                            success: true,
                            message: `Folder ${prefix} did not exist, so there was nothing to delete.`,
                            failed: 0,
                            succeeded: 0,
                            fileDeletionErrors: [],
                        };
                    }

                    this.ctx.verboseLog(
                        `Found ${filePaths.length} file(s) in folder ${prefix}`,
                        "info"
                    );

                    const { succeeded, fileDeletionErrors } =
                        await this.deleteObjects(
                            filePaths,
                            "S3FileManager.deleteFolder"
                        );

                    if (fileDeletionErrors.length > 0) {
                        return {
                            success: true,
                            message: `Some files in folder ${prefix} could not be deleted.`,
                            failed: fileDeletionErrors.length,
                            succeeded,
                            fileDeletionErrors,
                        };
                    } else if (fileDeletionErrors.length === filePaths.length) {
                        return {
                            success: false,
                            message: `Failed to delete all files contained in folder ${prefix}`,
                            failed: fileDeletionErrors.length,
                            succeeded,
                            fileDeletionErrors,
                        };
                    } else {
                        return {
                            success: true,
                            message: `Folder ${prefix} and the ${filePaths.length} files contained in it successfully deleted.`,
                            failed: 0,
                            succeeded,
                            fileDeletionErrors: [],
                        };
                    }
                } catch (error) {
                    throw new Error(
                        `An error occurred while attempting to delete folder ${prefix}: ${this.ctx.errorString(
                            error
                        )}`
                    );
                }
            }
        );
        return result;
    }

    // ════════════════════════════════════════════════════════════════
    // 🚮 DELETE OBJECTS (BATCH)
    // Performs a batch delete of multiple S3 objects
    // ════════════════════════════════════════════════════════════════

    private async deleteObjects(
        filePaths: string[],
        callerName: string
    ): Promise<{
        succeeded: number;
        fileDeletionErrors: FileDeletionError[];
    }> {
        const result = await this.ctx.withSpan(
            `${callerName} > deleteObjects`,
            {
                bucket: this.ctx.bucketName,
                numberOfObjects: filePaths.length,
            },
            async () => {
                const fileDeletionErrors: FileDeletionError[] = [];
                let succeeded: number = 0;

                let filePathBatch: string[] = [];
                for (let i = 0; i < filePaths.length; i++) {
                    filePathBatch.push(filePaths[i]);

                    if (
                        filePathBatch.length === 1000 ||
                        i === filePaths.length - 1
                    ) {
                        this.ctx.verboseLog(
                            `Attempting batch delete of ${filePathBatch.length} object(s)`,
                            "info"
                        );
                        const command = new DeleteObjectsCommand({
                            Bucket: this.ctx.bucketName,
                            Delete: {
                                Objects: filePathBatch.map((path) => ({
                                    Key: path,
                                })),
                                Quiet: false,
                            },
                        });
                        let attempt = 0;
                        while (true) {
                            try {
                                attempt++;
                                const response = await this.ctx.s3.send(
                                    command
                                );
                                const { Deleted, Errors } = response;

                                Errors?.forEach((err) => {
                                    if (this.ctx.allowVerboseLogging) {
                                        this.ctx.verboseLog(
                                            this.ctx.errorString(err),
                                            "warn"
                                        );
                                    }
                                    fileDeletionErrors.push({
                                        filePath: err.Key!,
                                        error: err,
                                    });
                                });

                                this.ctx.verboseLog(
                                    `Batch delete successful: ${
                                        Deleted?.length ?? 0
                                    } item(s) deleted`,
                                    "info"
                                );
                                succeeded += Deleted?.length ?? 0;
                                filePathBatch = [];
                                break;
                            } catch (error) {
                                this.ctx.handleRetryErrorLogging(
                                    attempt,
                                    "to delete objects",
                                    error
                                );
                                await wait(backoffDelay(attempt));
                            }
                        }
                    }
                }
                return {
                    succeeded,
                    fileDeletionErrors,
                };
            }
        );

        return result;
    }
}
