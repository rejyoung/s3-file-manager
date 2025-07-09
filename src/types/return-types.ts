import { _Error } from "@aws-sdk/client-s3";

export interface DeleteReturnType {
    success: boolean;
    deleted: boolean;
    filePath: string;
    reason?: string;
}

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

export interface DownloadAllReturnType {
    success: boolean;
    message: string;
    downloadedFiles: number;
    failedToDownload: string[];
}
