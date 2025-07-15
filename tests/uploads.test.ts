import { describe, it, expect, vi, beforeEach } from "vitest";
import { Readable } from "stream";
import {
    S3Client,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    PutObjectCommand,
    AbortMultipartUploadCommand,
} from "@aws-sdk/client-s3";
import { mockClient } from "aws-sdk-client-mock";
import { UploadManager } from "../src/core/uploadManager";
import { S3FMContext } from "../src/core/context";
import { FilePayload, UploadOptions } from "../src/types/input-types";
import fs from "fs";

// -- a tiny helper to create a buffer or stream of given length
function makeBufferOfSize(n: number): Buffer {
    return Buffer.alloc(n, "a");
}
function makeStreamOfSize(n: number): Readable {
    const buf = Buffer.alloc(n, "b");
    return Readable.from(buf);
}

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
    maxUploadPartSize: 10 * 1024 * 1024,
    maxUploadConcurrency: 4,
} as unknown as S3FMContext;

// Override s3 with mockS3
Object.defineProperty(mockCtx, "s3", { value: s3Mock, writable: false });

describe("UploadManager", () => {
    let ctx: S3FMContext;
    let uploads: UploadManager;

    beforeEach(() => {
        s3Mock.reset();
        uploads = new UploadManager(mockCtx, 2);

        s3Mock
            .on(CreateMultipartUploadCommand)
            .resolves({ UploadId: "test-upload-id" });
        s3Mock.on(UploadPartCommand).callsFake((params) => ({
            ETag: `"etag-${params.PartNumber}"`,
        }));
        s3Mock.on(CompleteMultipartUploadCommand).resolves({});
        s3Mock.on(PutObjectCommand).resolves({});
        s3Mock.on(AbortMultipartUploadCommand).resolves({});
    });

    it("should do a simple PUT for zero-length payload", async () => {
        const file: FilePayload = {
            name: "empty.txt",
            content: makeBufferOfSize(0),
        };
        const opts: UploadOptions = {};
        await uploads.uploadFile(file, opts);
        expect(s3Mock.commandCalls(PutObjectCommand).length).toBe(1);
        const cmd = s3Mock.commandCalls(PutObjectCommand)[0].args[0];
        // Body is the buffer
        expect(cmd.input.Body).toBeInstanceOf(Buffer);
        expect((cmd.input.Body as Buffer).length).toBe(0);
    });

    it("should choose simpleUpload for under‐threshold buffer", async () => {
        const small = makeBufferOfSize(mockCtx.maxUploadPartSize - 1);
        await uploads.uploadFile({ name: "small.bin", content: small }, {});
        expect(s3Mock.commandCalls(PutObjectCommand).length).toBe(1);
        const cmd = s3Mock.commandCalls(PutObjectCommand)[0].args[0];
        expect(cmd).toBeInstanceOf(PutObjectCommand);
    });

    it("should multipart‐upload a buffer in >threshold chunks", async () => {
        // Create a buffer of exactly 3×threshold + half threshold
        const size =
            mockCtx.maxUploadPartSize * 3 + mockCtx.maxUploadPartSize / 2;
        const buf = makeBufferOfSize(size);
        await uploads.uploadFile({ name: "big.bin", content: buf }, {});

        // First call: CreateMultipartUploadCommand
        expect(s3Mock.commandCalls(CreateMultipartUploadCommand).length).toBe(
            1
        );

        // Then some UploadPartCommands…
        const parts = s3Mock.commandCalls(UploadPartCommand);
        expect(parts.length).toBe(Math.ceil(size / mockCtx.maxUploadPartSize));

        // Finally a CompleteMultipartUploadCommand
        expect(s3Mock.commandCalls(CompleteMultipartUploadCommand).length).toBe(
            1
        );
    });

    it("should multipart‐upload a stream without buffering all at once", async () => {
        const size = mockCtx.maxUploadPartSize * 2 + 1234;
        const stream = makeStreamOfSize(size);
        await uploads.uploadFile({ name: "stream.bin", content: stream }, {});

        // Same expectations as above
        expect(s3Mock.commandCalls(CreateMultipartUploadCommand).length).toBe(
            1
        );
        const partCalls = s3Mock.commandCalls(UploadPartCommand);
        expect(partCalls.length).toBe(
            Math.ceil(size / mockCtx.maxUploadPartSize)
        );
        expect(s3Mock.commandCalls(CompleteMultipartUploadCommand).length).toBe(
            1
        );
    });

    it("bufferToChunks splits exactly at the threshold", () => {
        const buf = makeBufferOfSize(mockCtx.maxUploadPartSize * 2);
        const chunks = uploads["bufferToChunks"](buf);
        expect(chunks).toHaveLength(2);
        expect(chunks[0].length).toBe(mockCtx.maxUploadPartSize);
        expect(chunks[1].length).toBe(mockCtx.maxUploadPartSize);
    });
    it("should buffer and upload a small file from disk", async () => {
        const fakePath = "/tmp/small.txt";
        const fakeContent = Buffer.from("abc");
        const fakeStats = { size: fakeContent.length };

        vi.spyOn(fs, "statSync").mockReturnValue(fakeStats as fs.Stats);
        vi.spyOn(fs, "readFileSync").mockReturnValue(fakeContent);

        const spyUploadFile = vi
            .spyOn(uploads, "uploadFile")
            .mockResolvedValue("uploaded/small.txt");

        const result = await uploads.uploadFromDisk(fakePath);
        expect(result).toBe("uploaded/small.txt");

        expect(fs.statSync).toHaveBeenCalledWith(fakePath);
        expect(fs.readFileSync).toHaveBeenCalledWith(fakePath);
        expect(spyUploadFile).toHaveBeenCalledWith(
            {
                name: "small.txt",
                content: fakeContent,
                sizeHintBytes: fakeContent.length,
            },
            expect.anything()
        );
    });

    it("should stream and upload a large file from disk", async () => {
        const fakePath = "/tmp/large.txt";
        const largeSize = mockCtx.maxUploadPartSize + 1;
        const fakeStats = { size: largeSize };

        vi.spyOn(fs, "statSync").mockReturnValue(fakeStats as fs.Stats);
        const fakeStream = {
            ...Readable.from(Buffer.alloc(largeSize)),
            fakePath,
            close: vi.fn(),
            bytesRead: 0,
            pending: false,
        } as unknown as fs.ReadStream;
        vi.spyOn(fs, "createReadStream").mockReturnValue(fakeStream);

        const spyUploadFile = vi
            .spyOn(uploads, "uploadFile")
            .mockResolvedValue("uploaded/large.txt");

        const result = await uploads.uploadFromDisk(fakePath);
        expect(result).toBe("uploaded/large.txt");

        expect(fs.statSync).toHaveBeenCalledWith(fakePath);
        expect(fs.createReadStream).toHaveBeenCalledWith(fakePath);
        expect(spyUploadFile).toHaveBeenCalledWith(
            {
                name: "large.txt",
                content: fakeStream,
                sizeHintBytes: largeSize,
            },
            expect.anything()
        );
    });

    it("should prepare multiple streams from disk and upload them", async () => {
        const filePaths = ["/tmp/one.txt", "/tmp/two.txt"];
        const fakeStats = { size: 5000 };
        const fakeStreams = filePaths.map(
            (path) =>
                ({
                    ...Readable.from("test content"),
                    path,
                    close: vi.fn(),
                    bytesRead: 0,
                    pending: false,
                } as unknown as fs.ReadStream)
        );

        vi.spyOn(fs.promises, "stat").mockImplementation(
            async () => fakeStats as fs.Stats
        );
        vi.spyOn(fs, "createReadStream").mockImplementation(
            () => fakeStreams.shift()!
        );

        const spyUploadMultiple = vi
            .spyOn(uploads, "uploadMultipleFiles")
            .mockResolvedValue({
                filePaths: ["uploaded/one.txt", "uploaded/two.txt"],
                skippedFiles: [],
            });

        const result = await uploads.uploadMultipleFromDisk(filePaths);
        expect(result.filePaths).toEqual([
            "uploaded/one.txt",
            "uploaded/two.txt",
        ]);

        for (const path of filePaths) {
            expect(fs.promises.stat).toHaveBeenCalledWith(path);
            expect(fs.createReadStream).toHaveBeenCalledWith(path);
        }

        expect(spyUploadMultiple).toHaveBeenCalledWith(
            [
                expect.objectContaining({
                    name: "one.txt",
                    sizeHintBytes: fakeStats.size,
                }),
                expect.objectContaining({
                    name: "two.txt",
                    sizeHintBytes: fakeStats.size,
                }),
            ],
            expect.anything()
        );
    });
});
