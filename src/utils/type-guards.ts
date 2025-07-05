export type StreamType = "Blob" | "Readable" | "ReadableStream";
export const isStreamType = (t: string): t is StreamType => {
    return t === "Blob" || t === "Readable" || t === "ReadableStream";
};
