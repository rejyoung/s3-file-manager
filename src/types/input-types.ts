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
