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

export interface ListFoldersOptions
    extends Omit<ListItemsOptionsInternal, "directoriesOnly" | "spanOptions"> {
    spanOptions?: SpanOptions;
}
export interface ListFilesOptions extends ListFoldersOptions {}

export interface VerifyFilesOptions extends BasicOptions {
    prefix?: string;
}

export interface FilePayload {
    name: string;
    content: string | Buffer | Uint8Array | Blob | Readable | ReadableStream;
    contentType?: string; // mime-type
    sizeHintBytes?: number;
}

export type Stream = Blob | Readable | ReadableStream;

export interface UploadOptions extends BasicOptions {
    prefix?: string;
}

export interface GetStreamOptions extends BasicOptions {
    timeoutMS?: number;
}

export interface DownloadFileOptions extends BasicOptions {}
export interface DownloadToDiskOptions extends BasicOptions {
    outputFilename?: string;
}

export interface GetDownloadUrlOptions extends BasicOptions {
    expiresInSec?: number;
}

export interface DownloadFolderOptions extends BasicOptions {
    extensionOverride?: string;
}

export interface CopyFileOptions extends BasicOptions {
    sourceBucketName?: string;
}

export interface MoveFileOptions extends CopyFileOptions {}

export interface DeleteFileOptions extends BasicOptions {}

export interface DeleteFolderOptions extends BasicOptions {}

export interface RenameFileOptions extends BasicOptions {}
