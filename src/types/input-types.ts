import { Readable } from "stream";
import { Buffer } from "buffer";
import { ListItemsOptionsInternal } from "./internal-types.js";

export interface SpanOptions {
    name?: string;
    attributes?: Record<string, any>;
}

export interface BasicOptions {
    spanOptions?: SpanOptions;
}

export interface ListDirectoriesOptions
    extends Omit<ListItemsOptionsInternal, "directoriesOnly" | "spanOptions"> {
    spanOptions?: SpanOptions;
}
export interface ListFilesOptions extends ListDirectoriesOptions {}

export interface ConfirmFilesOptions extends BasicOptions {
    prefix?: string;
}

export interface FilePayload {
    name: string;
    content: string | Buffer | Uint8Array | Blob | Readable | ReadableStream;
}

export interface BatchedFilePayload extends FilePayload {
    contentType?: string;
    sizeHint?: number;
}

export type Stream = Blob | Readable | ReadableStream;

export interface FileUploadOptions extends BasicOptions {
    prefix?: string;
    contentType?: string; // mime-type
    sizeHint?: number;
}

export interface FileBatchUploadOptions
    extends Omit<FileUploadOptions, "sizeHint"> {}

export interface FileStreamOptions extends BasicOptions {
    timeoutMS?: number;
}

export interface LoadFileOptions extends BasicOptions {}
export interface DownloadToDiskOptions extends BasicOptions {
    outputFilename?: string;
}

export interface DownloadLinkOptions extends BasicOptions {
    expiresInSec?: number;
}

export interface DownloadAllOptions extends BasicOptions {
    extensionOverride?: string;
}

export interface CopyFileOptions extends BasicOptions {
    sourceBucketName?: string;
}

export interface MoveFileOptions extends CopyFileOptions {}

export interface DeleteFileOptions extends CopyFileOptions {}

export interface DeleteFolderOptions extends BasicOptions {}

export interface RenameFileOptions extends BasicOptions {}
