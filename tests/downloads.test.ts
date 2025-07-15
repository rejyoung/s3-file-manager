import { describe, it, expect, vi, beforeEach, MockInstance } from "vitest";
vi.mock("@aws-sdk/s3-request-presigner", () => ({
    getSignedUrl: vi.fn(),
}));
// Mock fs modules for Vitest (ESM)
vi.mock("fs/promises", () => ({
    writeFile: vi.fn(),
    mkdir: vi.fn(),
}));
vi.mock("fs", () => ({
    createWriteStream: vi.fn(),
}));
// Mock pipeline from stream/promises so Vitest can spy on it
vi.mock("stream/promises", () => ({
    pipeline: vi.fn(),
}));
import { mockClient } from "aws-sdk-client-mock";
import { writeFile } from "fs/promises";
import { createWriteStream } from "fs";
import { pipeline } from "stream/promises";
import { S3FMContext } from "../src/core/context";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { DownloadManager } from "../src/core/downloadManager";
import { PassThrough } from "stream";
import { IncomingMessage } from "http";
import { SdkStreamMixin } from "@aws-sdk/types";
import * as presigner from "@aws-sdk/s3-request-presigner";

const s3Mock = mockClient(S3Client);
const mockCtx = {
    bucketName: "test-bucket",
    s3: s3Mock,
    withSpan: async (_name: string, _attrs: any, fn: Function) => await fn(),
    verboseLog: vi.fn(),
    handleRetryErrorLogging: vi.fn(),
    logger: {
        info: vi.fn(),
        warn: vi.fn(),
    },
    listItems: vi.fn(),
    errorString: vi.fn(),
} as unknown as S3FMContext;

describe("DownloadManager", () => {
    let downloads: DownloadManager;

    beforeEach(() => {
        s3Mock.reset();
        vi.clearAllMocks();
        downloads = new DownloadManager(mockCtx);
    });

    // ========== getStream tests ==========
    describe("getStream", () => {
        it("should return a Readable stream to a file in the S3 bucket", async () => {
            const bodyStream = new PassThrough();
            bodyStream.end(Buffer.from("hello world"));
            // Cast Node stream to AWS SDK streaming type
            const sdkStream = bodyStream as unknown as IncomingMessage &
                SdkStreamMixin;
            s3Mock.on(GetObjectCommand).resolves({
                Body: sdkStream,
                ContentLength: Buffer.byteLength("hello world"),
            });

            const stream = await downloads.getStream("hello/world.txt", {
                timeoutMS: 8000,
            });
            const chunks: Buffer[] = [];
            for await (const chunk of stream as any) {
                chunks.push(chunk);
            }
            const data = Buffer.concat(chunks).toString("utf-8");
            expect(data).toBe("hello world");
        });
    });

    // ========== getTemporaryDownloadUrl tests ==========
    describe("getTemporaryDownloadUrl", () => {
        it("should return a presigned url with the expected expiration date", async () => {
            (
                presigner.getSignedUrl as unknown as ReturnType<typeof vi.fn>
            ).mockResolvedValue("https://example.com/fake");

            const url = await downloads.getTemporaryDownloadUrl(
                "hello/world.txt",
                { expiresInSec: 1234 }
            );

            expect(presigner.getSignedUrl).toHaveBeenCalledWith(
                mockCtx.s3,
                expect.any(GetObjectCommand),
                { expiresIn: 1234 }
            );
            expect(url).toBe("https://example.com/fake");
        });
    });

    // ========== downloadFile tests ==========
    describe("downloadFile", () => {
        it("should return text when fileType is 'text'", async () => {
            // Stub getStream to return a stream with "hello text"
            const textBuf = Buffer.from("hello text");
            const textStream = new PassThrough();
            textStream.end(textBuf);
            vi.spyOn(downloads, "getStream").mockResolvedValue(
                textStream as any
            );
            // Stub streamToBuffer to return the raw Buffer
            (mockCtx as any).streamToBuffer = vi
                .fn()
                .mockResolvedValue(textBuf);
            // Stub getFileFormat to return text type
            vi.spyOn(downloads as any, "getFileFormat").mockResolvedValue({
                fileType: "text",
                extension: "",
            });

            const result = await downloads.downloadFile("foo.txt", {});
            expect(result).toBe("hello text");
        });

        it("should return JSON object when fileType is 'json'", async () => {
            const jsonObj = { a: 1 };
            const jsonBuf = Buffer.from(JSON.stringify(jsonObj));
            const jsonStream = new PassThrough();
            jsonStream.end(jsonBuf);
            vi.spyOn(downloads, "getStream").mockResolvedValue(
                jsonStream as any
            );
            (mockCtx as any).streamToBuffer = vi
                .fn()
                .mockResolvedValue(jsonBuf);
            vi.spyOn(downloads as any, "getFileFormat").mockResolvedValue({
                fileType: "json",
                extension: "",
            });

            const result = await downloads.downloadFile("foo.json", {});
            expect(result).toEqual(jsonObj);
        });

        it("should return Buffer when fileType is 'buffer'", async () => {
            const buf = Buffer.from("raw data");
            const bufStream = new PassThrough();
            bufStream.end(buf);
            vi.spyOn(downloads, "getStream").mockResolvedValue(
                bufStream as any
            );
            (mockCtx as any).streamToBuffer = vi.fn().mockResolvedValue(buf);
            vi.spyOn(downloads as any, "getFileFormat").mockResolvedValue({
                fileType: "buffer",
                extension: "",
            });

            const result = await downloads.downloadFile("foo.bin", {});
            expect(Buffer.isBuffer(result)).toBe(true);
            expect(result).toEqual(buf);
        });
    });

    // ========== downloadToDisk tests ==========
    describe("downloadToDisk", () => {
        it("should use writeFile for small files", async () => {
            // Stub getFileMetadata to return small contentLength
            vi.spyOn(downloads as any, "getFileMetadata").mockResolvedValue({
                mimeType: "application/octet-stream",
                contentLength: 1,
            });

            // Stub getStream to return a dummy readable stream
            const dummyStream = new PassThrough();
            dummyStream.end(Buffer.alloc(0));
            vi.spyOn(downloads, "getStream").mockResolvedValue(
                dummyStream as any
            );

            // Stub ctx.streamToBuffer to return a buffer
            vi.spyOn(mockCtx, "streamToBuffer").mockResolvedValue(
                Buffer.from("abc")
            );

            // Ensure our mocked writeFile resolves
            (
                writeFile as unknown as ReturnType<typeof vi.fn>
            ).mockResolvedValue(undefined);

            await downloads.downloadToDisk("foo.txt", "/tmp", {
                spanOptions: {},
            });

            expect(writeFile).toHaveBeenCalledWith(
                "/tmp/foo.bin",
                Buffer.from("abc")
            );
        });

        it("should use pipeline for large files", async () => {
            vi.spyOn(downloads as any, "getFileMetadata").mockResolvedValue({
                mimeType: "application/octet-stream",
                contentLength: 300 * 1024 * 1024,
            });

            // Create a dummy stream
            const dummyStream = new PassThrough();

            vi.spyOn(downloads, "getStream").mockResolvedValue(
                dummyStream as any
            );

            // Ensure our mocked pipeline resolves
            (pipeline as unknown as ReturnType<typeof vi.fn>).mockResolvedValue(
                undefined
            );

            await downloads.downloadToDisk("big.bin", "/outDir/", {
                spanOptions: {},
            });

            expect(pipeline).toHaveBeenCalled();
            expect(createWriteStream).toHaveBeenCalledWith("/outDir/big.bin");
        });
    });

    // ========== downloadFolderToDisk tests ==========
    describe("downloadFolderToDisk", () => {
        it("should return success when no files", async () => {
            vi.spyOn(mockCtx, "listItems").mockResolvedValue([]);
            const res = await downloads.downloadFolderToDisk("prefix", "/any", {
                spanOptions: {},
            });
            expect(res).toEqual({
                success: true,
                message: "No files found with prefix prefix",
                downloadedFiles: 0,
                failedToDownload: [],
            });
        });

        it("should report all success when downloads succeed", async () => {
            vi.spyOn(mockCtx, "listItems").mockResolvedValue(["a", "b"]);
            vi.spyOn(downloads, "downloadToDisk").mockResolvedValue(
                undefined as any
            );

            const res = await downloads.downloadFolderToDisk("pref", "/od", {
                spanOptions: {},
            });
            expect(res).toEqual({
                success: true,
                message: "All files with prefix pref successfully downloaded",
                downloadedFiles: 2,
                failedToDownload: [],
            });
        });

        it("should report partial failures", async () => {
            vi.spyOn(mockCtx, "listItems").mockResolvedValue(["x", "y"]);
            vi.spyOn(downloads, "downloadToDisk")
                .mockImplementationOnce(async () => Promise.resolve())
                .mockImplementationOnce(async () => {
                    throw new Error("fail");
                });

            const res = await downloads.downloadFolderToDisk("p", "/o", {
                spanOptions: {},
            });
            expect(res.success).toBe(true);
            expect(res.downloadedFiles).toBe(1);
            expect(res.failedToDownload).toEqual(["y"]);
        });
    });
});
