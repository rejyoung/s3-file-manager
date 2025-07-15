import { S3FileManager } from "./s3-file-manager.js";
import { FMConfig } from "./types/fmconfig-types.js";

export default S3FileManager;
export type { FMConfig };
export type {
    ListFilesOptions,
    ListFoldersOptions,
    VerifyFilesOptions,
    CopyFileOptions,
    MoveFileOptions,
    RenameFileOptions,
    DeleteFileOptions,
    DeleteFolderOptions,
    UploadOptions,
    GetStreamOptions,
    DownloadFileOptions,
    DownloadToDiskOptions,
    GetDownloadUrlOptions,
} from "./types/input-types.js";

export type {
    CopyReturnType,
    MoveReturnType,
    RenameReturnType,
    DeleteFolderReturnType,
    DownloadFolderReturnType,
    UploadFilesReturnType,
} from "./types/return-types.js";
