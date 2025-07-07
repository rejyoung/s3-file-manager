import { Readable } from "stream";
import { Buffer } from "buffer";

export interface SpanOptions {
    name?: string;
    attributes?: Record<string, any>;
}

export interface BasicOptions {
    spanOptions?: SpanOptions;
}

export interface ListFilesOptions extends BasicOptions {
    prefix?: string;
    filterFn?: (filename: string) => boolean;
    compareFn?: (a: string, b: string) => number;
}

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

export interface DeleteFileOptions extends BasicOptions {}
export interface LoadFileOptions extends BasicOptions {}
