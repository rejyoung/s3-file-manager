// tests/uploadManager.test.ts
import { Readable } from "stream";
import Bottleneck from "bottleneck";

import { UploadManager } from "../src/core/uploadManager";
import { S3FMContext } from "../src/core/context";
import { FilePayload, FileUploadOptions } from "../src/types/input-types";

// -- a tiny helper to create a buffer or stream of given length
function makeBufferOfSize(n: number): Buffer {
    return Buffer.alloc(n, "a");
}
function makeStreamOfSize(n: number): Readable {
    const buf = Buffer.alloc(n, "b");
    return Readable.from(buf);
}

describe("UploadManager", () => {
    let ctx: S3FMContext;
    let uploads: UploadManager;
    let sendMock: jest.Mock;

    beforeEach(() => {
        // stub out S3Client.send
        sendMock = jest.fn((command) => {
            const name = command.constructor.name;
            if (name === "CreateMultipartUploadCommand") {
                return Promise.resolve({ UploadId: "test-upload-id" });
            }
            if (name === "UploadPartCommand") {
                return Promise.resolve({
                    ETag: `"etag-${command.PartNumber}"`,
                });
            }
            if (name === "CompleteMultipartUploadCommand") {
                return Promise.resolve({});
            }
            if (name === "PutObjectCommand") {
                return Promise.resolve({});
            }
            if (name === "AbortMultipartUploadCommand") {
                return Promise.resolve({});
            }
            return Promise.resolve({});
        });
        ctx = new S3FMContext({
            bucketName: "my-bucket",
            bucketRegion: "us-east-1",
            endpoint: undefined,
            credentials: { accessKeyId: "x", secretAccessKey: "y" },
        });
        // @ts-ignore override the internal client
        ctx.s3.send = sendMock;

        uploads = new UploadManager(ctx, /* maxUploadConcurrency */ 2);
    });

    it("should do a simple PUT for zero-length payload", async () => {
        const file: FilePayload = {
            name: "empty.txt",
            content: makeBufferOfSize(0),
        };
        const opts: FileUploadOptions = {};
        await uploads.uploadFile(file, opts);
        expect(sendMock).toHaveBeenCalledTimes(1);
        const cmd = sendMock.mock.calls[0][0];
        expect(cmd.constructor.name).toBe("PutObjectCommand");
        // Body is the buffer
        expect((cmd as any).input.Body).toBeInstanceOf(Buffer);
        expect((cmd as any).input.Body.length).toBe(0);
    });

    it("should choose simpleUpload for under‐threshold buffer", async () => {
        const small = makeBufferOfSize(ctx.multipartThreshold - 1);
        await uploads.uploadFile({ name: "small.bin", content: small }, {});
        expect(sendMock).toHaveBeenCalledWith(expect.anything());
        const cmd = sendMock.mock.calls[0][0];
        expect(cmd.constructor.name).toBe("PutObjectCommand");
    });

    it("should multipart‐upload a buffer in >threshold chunks", async () => {
        // Create a buffer of exactly 3×threshold + half threshold
        const size = ctx.multipartThreshold * 3 + ctx.multipartThreshold / 2;
        const buf = makeBufferOfSize(size);
        await uploads.uploadFile({ name: "big.bin", content: buf }, {});

        // First call: CreateMultipartUploadCommand
        const first = sendMock.mock.calls[0][0];
        expect(first.constructor.name).toBe("CreateMultipartUploadCommand");

        // Then some UploadPartCommands…
        const parts = sendMock.mock.calls.filter(
            (c) => c[0].constructor.name === "UploadPartCommand"
        );
        expect(parts.length).toBe(Math.ceil(size / ctx.multipartThreshold));

        // Finally a CompleteMultipartUploadCommand
        const last = sendMock.mock.calls[sendMock.mock.calls.length - 1][0];
        expect(last.constructor.name).toBe("CompleteMultipartUploadCommand");
    });

    it("should multipart‐upload a stream without buffering all at once", async () => {
        const size = ctx.multipartThreshold * 2 + 1234;
        const stream = makeStreamOfSize(size);
        await uploads.uploadFile({ name: "stream.bin", content: stream }, {});

        // Same expectations as above
        expect(sendMock).toHaveBeenCalled();
        expect(sendMock.mock.calls[0][0].constructor.name).toBe(
            "CreateMultipartUploadCommand"
        );
        const partCalls = sendMock.mock.calls.filter(
            (c) => c[0].constructor.name === "UploadPartCommand"
        );
        expect(partCalls.length).toBe(Math.ceil(size / ctx.multipartThreshold));
        expect(sendMock.mock.calls.slice(-1)[0][0].constructor.name).toBe(
            "CompleteMultipartUploadCommand"
        );
    });

    it("bufferToChunks splits exactly at the threshold", () => {
        const buf = makeBufferOfSize(ctx.multipartThreshold * 2);
        const chunks = uploads["bufferToChunks"](buf);
        expect(chunks).toHaveLength(2);
        expect(chunks[0].length).toBe(ctx.multipartThreshold);
        expect(chunks[1].length).toBe(ctx.multipartThreshold);
    });
});
