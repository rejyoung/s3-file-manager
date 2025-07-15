import { _Error } from "@aws-sdk/client-s3";

export interface CopyReturnType {
    success: boolean;
    source: string;
    destination: string;
}

export interface MoveReturnType {
    success: boolean;
    source: string;
    destination: string;
    originalDeleted: boolean;
}

export interface RenameReturnType {
    success: boolean;
    oldPath: string;
    newPath: string;
    originalDeleted: boolean;
}

export interface DeleteFolderReturnType {
    success: boolean;
    message: string;
    failed: number;
    succeeded: number;
    fileDeletionErrors: FileDeletionError[];
}

export interface FileDeletionError {
    filePath: string;
    error: _Error;
}

export interface DownloadFolderReturnType {
    success: boolean;
    message: string;
    downloadedFiles: number;
    failedToDownload: string[];
}

export interface UploadFilesReturnType {
    filePaths: string[];
    skippedFiles: string[];
}
