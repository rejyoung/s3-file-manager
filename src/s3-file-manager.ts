import { FMConfig } from "./types/fmconfig-types.js";
import {
    VerifyFilesOptions,
    CopyFileOptions,
    DeleteFileOptions,
    DeleteFolderOptions,
    DownloadFolderOptions,
    GetDownloadUrlOptions,
    DownloadToDiskOptions,
    FilePayload,
    GetStreamOptions,
    UploadOptions,
    ListFoldersOptions,
    ListFilesOptions,
    DownloadFileOptions,
    MoveFileOptions,
    RenameFileOptions,
} from "./types/input-types.js";
import { S3FMContext } from "./core/context.js";
import { UploadManager } from "./core/uploadManager.js";
import { FileService } from "./core/fileService.js";
import { DownloadManager } from "./core/downloadManager.js";
import { Readable } from "stream";
import {
    CopyReturnType,
    DeleteFolderReturnType,
    DownloadFolderReturnType,
    MoveReturnType,
    RenameReturnType,
    UploadFilesReturnType,
} from "./types/return-types.js";

/**
 * ╔════════════════════════════════════════════════════════════════════════════╗
 * ║ 📦 S3 FILE MANAGER                                                         ║
 * ║ Primary facade class for interacting with S3. Provides unified public      ║
 * ║ methods for uploading, downloading, and inspecting files.                  ║
 * ║ Internally delegates to UploadManager, DownloadManager, and FileService.   ║
 * ╚════════════════════════════════════════════════════════════════════════════╝
 */

export class S3FileManager {
    private readonly sharedContext: S3FMContext;
    private readonly uploads: UploadManager;
    private readonly downloads: DownloadManager;
    private readonly services: FileService;

    constructor(config: FMConfig) {
        this.sharedContext = new S3FMContext(config);
        this.uploads = new UploadManager(
            this.sharedContext,
            config.maxUploadConcurrency
        );
        this.downloads = new DownloadManager(this.sharedContext);
        this.services = new FileService(this.sharedContext);
    }

    /**
 ╔════════════════════════════════════════════════════════════════════════════════╗
 ║ 🧾 FILE SERVICE                                                                ║
 ║ Public-facing interface for loading, saving, or transferring file data         ║
 ║ through the S3 storage layer. Orchestrates upload/download logic.              ║
 ╚════════════════════════════════════════════════════════════════════════════════╝
 */

    /**
     * Lists all files in the S3 bucket or under a given prefix in the S3 bucket.
     * @param prefix - The S3 prefix under which to search
     * @param options - Optional settings including filter and compare functions and tracing span settings.
     * If prefix is not supplied, all file paths in bucket are listed.
     * @returns A promise resolving to an array of file paths.
     */
    public async listFiles(
        prefix: string,
        options?: ListFilesOptions
    ): Promise<string[]> {
        return this.services.listFiles(prefix, options);
    }

    /**
     * Lists all directories (common prefixes) in the S3 bucket or within a given prefix in the S3 bucket.
     * @param prefix - The S3 prefix under which to search
     * @param options - Optional settings including filter and compare functions and tracing span settings.
     * If prefix is not supplied, all directories in bucket are listed.
     * @returns Promise resolving to an array of directory names (prefixes).
     */
    public async listFolders(
        prefix: string,
        options?: ListFoldersOptions
    ): Promise<string[]> {
        return this.services.listFolders(prefix, options);
    }

    /**
     * Verifies the existence of multiple files in the S3 bucket.
     * @param filePaths - An array of S3 file paths to check.
     * @param options - Optional settings including prefix and tracing span settings.
     * @returns A promise resolving to an array of missing file paths.
     */
    public async verifyFilesExist(
        filenames: string[],
        options?: VerifyFilesOptions
    ): Promise<string[]> {
        if (options) {
            delete (options as any).bucketName;
        }
        return this.services.verifyFilesExist(filenames, options);
    }

    /**
     * Copies a file from one location to another within the same bucket or from a different bucket to the current bucket.
     * Optionally allows renaming the file during the copy operation.
     * @param filePath - Path to the file in the bucket to copy.
     * @param destinationPrefix - The prefix to copy the file to.
     * @param options - Optional settings including source bucket name and tracing span settings.
     * @returns A promise resolving to a CopyReturnType object indicating success, source path, and destination path.
     */
    public async copyFile(
        filePath: string,
        destinationPrefix: string,
        options?: CopyFileOptions
    ): Promise<CopyReturnType> {
        if (options) {
            delete (options as any).newFilename;
        }
        return await this.services.copyFile(
            filePath,
            destinationPrefix,
            options
        );
    }

    /**
     * Moves a file from its current location to a new destination prefix.
     * This operation copies the file and then deletes the original.
     * If the copy succeeds but the delete fails, the copied file remains.
     * @param filePath - Path to the file in the bucket to move.
     * @param destinationPrefix - The new prefix/location where the file should be moved.
     * @param options - Optional settings including source bucket name and tracing span settings.
     * @returns A promise resolving to a MoveReturnType object indicating success, source and destination, and whether original was deleted.
     */
    public async moveFile(
        filePath: string,
        destinationPrefix: string,
        options?: MoveFileOptions
    ): Promise<MoveReturnType> {
        return await this.services.moveFile(
            filePath,
            destinationPrefix,
            options
        );
    }

    /**
     * Renames a file within the same directory by copying it to a new filename and deleting the original.
     * @param filePath - Path to the file in the bucket to rename.
     * @param newFilename - The new filename to use.
     * @param options - Optional tracing span settings.
     * @returns A promise resolving to a RenameReturnType object indicating success, old and new path, and whether original was deleted.
     */
    public async renameFile(
        filePath: string,
        newFilename: string,
        options?: RenameFileOptions
    ): Promise<RenameReturnType> {
        return await this.services.renameFile(filePath, newFilename, options);
    }

    /**
     * Deletes a file from the S3 bucket. Handles missing files gracefully.
     * @param filePath - Path to the file in the bucket to delete.
     * @param options - Optional tracing span settings.
     * @returns A promise resolving to a DeleteReturnType object indicating success, deletion status, and file path.
     */
    public async deleteFile(
        filePath: string,
        options?: DeleteFileOptions
    ): Promise<void> {
        if (options) {
            delete (options as any).bucketName;
        }

        return await this.services.deleteFile(filePath, options);
    }

    /**
     * Deletes all objects under the given prefix (folder) in the S3 bucket.
     *
     * @param prefix - The S3 key prefix representing the folder to delete.
     * @param options - Optional settings including tracing span options.
     * @returns A promise resolving to a DeleteFolderReturnType object containing:
     *   - success: boolean indicating if the folder deletion succeeded.
     *   - failed: number of objects that failed to delete.
     *   - succeeded: number of objects successfully deleted.
     *   - fileDeletionErrors: array of error details for any failed deletions.
     */
    public async deleteFolder(
        prefix: string,
        options?: DeleteFolderOptions
    ): Promise<DeleteFolderReturnType> {
        return await this.services.deleteFolder(prefix, options);
    }

    /**
 ╔════════════════════════════════════════════════════════════════════════════════╗
 ║ 📤 UPLOAD MANAGER                                                              ║
 ║ Handles file uploads to S3, including direct uploads, multipart uploads, and   ║
 ║ retry logic for reliability.                                                   ║
 ╚════════════════════════════════════════════════════════════════════════════════╝
 */

    /**
     * Uploads a file to the configured S3 bucket.
     * Automatically chooses between single and multipart upload based on size.
     * @param file - Object containing file name, content (Buffer or Readable), optional MIME type, and optional size hint.
     * @param options - Optional settings including prefix and spanOptions.
     * @returns A promise that resolves when the file is successfully uploaded.
     */

    public async uploadFile(
        file: FilePayload,
        options?: UploadOptions
    ): Promise<string> {
        return await this.uploads.uploadFile(file, options);
    }

    /**
     * Uploads multiple files concurrently to S3.
     * Gracefully logs and skips failed uploads without halting the entire batch.
     * @param files - Array of file objects, each containing file name, content (Buffer or Readable), optional MIME type, and optional size hint.
     * @param options - Optional settings including prefix and spanOptions.
     * @returns A promise resolving to a list of files that failed to upload.
     */

    public async uploadMultipleFiles(
        files: FilePayload[],
        options?: UploadOptions
    ): Promise<UploadFilesReturnType> {
        return await this.uploads.uploadMultipleFiles(files, options);
    }

    /**
     * Uploads a single file from a local disk path to the S3 bucket.
     * Automatically determines file name and stream type.
     * @param localFilePath - Absolute or relative path to the local file to upload.
     * @param options - Optional settings including prefix and spanOptions.
     * @returns A promise resolving to the S3 key (path) of the uploaded file.
     */
    public async uploadFromDisk(
        localFilePath: string,
        options: UploadOptions = {}
    ): Promise<string> {
        return await this.uploads.uploadFromDisk(localFilePath, options);
    }

    /**
     * Uploads multiple local files to S3 in parallel.
     * Gracefully logs and skips failed uploads without halting the entire batch.
     * Automatically determines file names and stream types for each file.
     * @param localFilePaths - Array of absolute or relative paths to local files.
     * @param options - Optional settings including prefix and spanOptions.
     * @returns A promise resolving to an object containing an array of failed file paths.
     */
    public async uploadMultipleFromDisk(
        localFilePaths: string[],
        options: UploadOptions = {}
    ): Promise<UploadFilesReturnType> {
        return await this.uploads.uploadMultipleFromDisk(
            localFilePaths,
            options
        );
    }

    /**
 ╔════════════════════════════════════════════════════════════════════════════════╗
 ║ 📥 DOWNLOAD MANAGER                                                            ║
 ║ Manages downloads from S3, supporting buffered and streamed file retrieval,    ║
 ║ with support for metadata extraction and type detection.                       ║
 ╚════════════════════════════════════════════════════════════════════════════════╝
 */

    /**
     * Streams a file from the S3 bucket using a readable stream.
     * @param filePath - Path to the file in the bucket.
     * @param options - Optional settings including custom timeout duration and tracing options.
     * @returns A readable stream of the file's contents.
     */
    public async getStream(
        filePath: string,
        options?: GetStreamOptions
    ): Promise<Readable> {
        return await this.downloads.getStream(filePath, options);
    }

    /**
     * Loads a file's contents into memory as a string, Buffer, or parsed object depending on type.
     * @param filePath - Path to the file in the bucket.
     * @param options - Optional tracing span settings.
     * @returns The file's contents as a string, object, or Buffer.
     */
    public async downloadFile(
        filePath: string,
        options?: DownloadFileOptions
    ): Promise<string | Buffer | object> {
        return await this.downloads.downloadFile(filePath, options);
    }

    /**
     * Downloads a file from S3 and saves it to a specified location on disk.
     * Determines file name and extension based on S3 metadata or user overrides.
     * @param filePath - Path to the file in the bucket.
     * @param outDir - Path to folder in which to save the file.
     * @param options - Optional output filename override and tracing span settings.
     * @returns A promise that resolves when the file is successfully uploaded.
     */
    public async downloadToDisk(
        filePath: string,
        outDir: string,
        options?: DownloadToDiskOptions
    ): Promise<void> {
        return await this.downloads.downloadToDisk(filePath, outDir, options);
    }

    /**
     * Downloads all files in the S3 bucket with the specified prefix to a local directory.
     *
     * @param prefix - The S3 key prefix to filter which files to download.
     * @param outDir - The local directory path where files will be saved.
     * @param options - Optional settings, including tracing span options.
     * @returns A promise that resolves to a DownloadFolderReturnType object containing:
     *   - success: boolean indicating if all required downloads succeeded (true if at least some files downloaded without all failing).
     *   - message: descriptive status message.
     *   - downloadedFiles: number of files successfully downloaded.
     *   - failedToDownload: array of file paths that failed to download.
     */
    public async downloadFolderToDisk(
        prefix: string,
        outDir: string,
        options?: DownloadFolderOptions
    ): Promise<DownloadFolderReturnType> {
        return await this.downloads.downloadFolderToDisk(
            prefix,
            outDir,
            options
        );
    }

    /**
     * Generates a presigned URL for temporary access to a file in S3.
     *
     * @param filePath - The S3 key (path) of the file for which to generate a link.
     * @param options - Optional settings including expiresInSec (expiration time in seconds) and tracing span options.
     * @returns A promise that resolves to a string containing the presigned URL.
     */
    public async getTemporaryDownloadUrl(
        filePath: string,
        options?: GetDownloadUrlOptions
    ): Promise<string> {
        return await this.downloads.getTemporaryDownloadUrl(filePath, options);
    }
}
