import { Readable } from "stream";
import { Buffer } from "buffer";

export interface ListFilesOptions {
    prefix?: string;
    filterFn?: (filename: string) => boolean;
    compareFn?: (a: string, b: string) => number;
    spanOptions?: SpanOptions;
}

export interface ConfirmFilesOptions {
    prefix?: string;
    filenames: string[];
    spanOptions?: SpanOptions;
}

export interface SpanOptions {
    name?: string;
    attributes?: Record<string, any>;
}

export interface FilePayload {
    name: string;
    content: string | Buffer | Uint8Array | Blob | Readable | ReadableStream;
}

export type Stream = Blob | Readable | ReadableStream;

export interface FileUploadOptions {
    prefix?: string;
    contentType?: string; // mime-type
    sizeHint?: number;
    spanOptions?: SpanOptions;
}
