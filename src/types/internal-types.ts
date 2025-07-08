import {
    ConfirmFilesOptions,
    CopyFileOptions,
    DeleteFileOptions,
    SpanOptions,
} from "./input-types.js";

export type UploadContentType =
    | "string"
    | "Buffer"
    | "Uint8Array"
    | "Blob"
    | "Readable"
    | "ReadableStream";

export type FileContentType = "text" | "buffer" | "json";

export type FileMetadata = {
    mimeType?: string;
    contentLength?: number;
};

export interface GetFileFormatInput {
    filePath: string;
    callerName: string;
    fileBuffer?: Buffer;
    mimeType?: string;
}

export interface CopyFileOptionsInternal extends CopyFileOptions {
    newFilename?: string;
}

export interface ConfirmFilesOptionsInternal extends ConfirmFilesOptions {
    bucketName?: string;
}

export interface DeleteFileOptionsInternal extends DeleteFileOptions {
    sourceBucketName?: string;
}

export interface ListItemsOptionsInternal {
    prefix?: string;
    filterFn?: (filename: string) => boolean;
    compareFn?: (a: string, b: string) => number;
    directoriesOnly?: boolean;
    spanOptions: SpanOptions;
}
