import { FMConfig } from "./types/fmconfig-types.js";
import {
    BatchedFilePayload,
    ConfirmFilesOptions,
    CopyFileOptions,
    DeleteFileOptions,
    DownloadToDiskOptions,
    FilePayload,
    FileStreamOptions,
    FileUploadOptions,
    ListDirectoriesOptions,
    ListFilesOptions,
    LoadFileOptions,
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
    DeleteReturnType,
    MoveReturnType,
    RenameReturnType,
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
     * Lists all files in the S3 bucket or under a given prefix in the S3 bucket.
     * @param options - Optional settings including prefix and tracing span settings.
     * If prefix is not supplied, all file paths in bucket are listed.
     * @returns A promise resolving to an array of file paths.
     */
    public async listFiles(options?: ListFilesOptions): Promise<string[]> {
        return this.services.listFiles(options);
    }

    /**
     * Lists all directories (common prefixes) in the S3 bucket or within a given prefix in the S3 bucket.
     *
     * @param options - Optional settings including prefix and tracing span settings.
     * If prefix is not supplied, all directories in bucket are listed.
     * @returns Promise resolving to an array of directory names (prefixes).
     */
    public async listDirectories(
        options?: ListDirectoriesOptions
    ): Promise<string[]> {
        return this.services.listDirectories(options);
    }

    /**
     * Confirms the existence of multiple files in the S3 bucket.
     * @param filePaths - An array of S3 file paths to check.
     * @param options - Optional settings including prefix and tracing span settings.
     * @returns A promise resolving to an array of missing file paths.
     */
    public async confirmFilesExist(
        filenames: string[],
        options?: ConfirmFilesOptions
    ): Promise<string[]> {
        if (options) {
            delete (options as any).bucketName;
        }
        return this.services.confirmFilesExist(filenames, options);
    }

    /**
     * Uploads a file to the configured S3 bucket.
     * Automatically chooses between single and multipart upload based on size.
     * @param file - Object containing file name and content (Buffer or Readable).
     * @param options - Optional settings including prefix, content type, and size hint.
     * @returns A promise that resolves when the file is successfully uploaded.
     */

    public async uploadFile(
        file: FilePayload,
        options?: FileUploadOptions
    ): Promise<void> {
        return await this.uploads.uploadFile(file, options);
    }

    /**
     * Uploads multiple files concurrently to S3.
     * Gracefully logs and skips failed uploads without halting the entire batch.
     * @param files - Array of file objects to upload.
     * @param options - Optional settings including prefix and content type.
     * @returns A promise resolving to a list of files that failed to upload.
     */

    public async uploadFileBatch(
        files: BatchedFilePayload[],
        options?: FileUploadOptions
    ): Promise<string[]> {
        return await this.uploads.uploadFileBatch(files, options);
    }

    /**
     * Streams a file from the S3 bucket using a readable stream.
     * @param filePath - Path to the file in the bucket.
     * @param options - Optional settings including custom timeout duration and tracing options.
     * @returns A readable stream of the file's contents.
     */
    public async streamFile(
        filePath: string,
        options?: FileStreamOptions
    ): Promise<Readable> {
        return await this.downloads.streamFile(filePath, options);
    }

    /**
     * Loads a file's contents into memory as a string, Buffer, or parsed object depending on type.
     * @param filePath - Path to the file in the bucket.
     * @param options - Optional tracing span settings.
     * @returns The file's contents as a string, object, or Buffer.
     */
    public async loadFile(
        filePath: string,
        options?: LoadFileOptions
    ): Promise<string | Buffer | object> {
        return await this.downloads.loadFile(filePath, options);
    }

    /**
     * Downloads a file from S3 and saves it to a specified location on disk.
     * Determines file name and extension based on S3 metadata or user overrides.
     * @param filePath - Path to the file in the bucket.
     * @param outDir - Path to folder in which to save the file.
     * @param options - Optional output filename override and tracing span settings.
     * @returns A promise resolving to the full local path of the saved file.
     */
    public async downloadToDisk(
        filePath: string,
        outDir: string,
        options?: DownloadToDiskOptions
    ): Promise<void> {
        return await this.downloadToDisk(filePath, outDir, options);
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
     * Deletes a file from the S3 bucket. Handles missing files gracefully.
     * @param filePath - Path to the file in the bucket to delete.
     * @param options - Optional tracing span settings.
     * @returns A promise resolving to a DeleteReturnType object indicating success, deletion status, and file path.
     */
    public async deleteFile(
        filePath: string,
        options?: DeleteFileOptions
    ): Promise<DeleteReturnType> {
        if (options) {
            delete (options as any).bucketName;
        }

        return await this.services.deleteFile(filePath, options);
    }
}
