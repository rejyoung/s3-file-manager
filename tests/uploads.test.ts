// tests/uploadManager.test.ts
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
    const s3Mock = mockClient(S3Client);

    beforeEach(() => {
        s3Mock.reset();
        ctx = new S3FMContext({
            bucketName: "my-bucket",
            bucketRegion: "us-east-1",
            endpoint: undefined,
            credentials: { accessKeyId: "x", secretAccessKey: "y" },
        });
        uploads = new UploadManager(ctx, 2);

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
        const opts: FileUploadOptions = {};
        await uploads.uploadFile(file, opts);
        expect(s3Mock.commandCalls(PutObjectCommand).length).toBe(1);
        const cmd = s3Mock.commandCalls(PutObjectCommand)[0].args[0];
        // Body is the buffer
        expect(cmd.input.Body).toBeInstanceOf(Buffer);
        expect((cmd.input.Body as Buffer).length).toBe(0);
    });

    it("should choose simpleUpload for under‐threshold buffer", async () => {
        const small = makeBufferOfSize(ctx.multipartThreshold - 1);
        await uploads.uploadFile({ name: "small.bin", content: small }, {});
        expect(s3Mock.commandCalls(PutObjectCommand).length).toBe(1);
        const cmd = s3Mock.commandCalls(PutObjectCommand)[0].args[0];
        expect(cmd).toBeInstanceOf(PutObjectCommand);
    });

    it("should multipart‐upload a buffer in >threshold chunks", async () => {
        // Create a buffer of exactly 3×threshold + half threshold
        const size = ctx.multipartThreshold * 3 + ctx.multipartThreshold / 2;
        const buf = makeBufferOfSize(size);
        await uploads.uploadFile({ name: "big.bin", content: buf }, {});

        // First call: CreateMultipartUploadCommand
        expect(s3Mock.commandCalls(CreateMultipartUploadCommand).length).toBe(
            1
        );

        // Then some UploadPartCommands…
        const parts = s3Mock.commandCalls(UploadPartCommand);
        expect(parts.length).toBe(Math.ceil(size / ctx.multipartThreshold));

        // Finally a CompleteMultipartUploadCommand
        expect(s3Mock.commandCalls(CompleteMultipartUploadCommand).length).toBe(
            1
        );
    });

    it("should multipart‐upload a stream without buffering all at once", async () => {
        const size = ctx.multipartThreshold * 2 + 1234;
        const stream = makeStreamOfSize(size);
        await uploads.uploadFile({ name: "stream.bin", content: stream }, {});

        // Same expectations as above
        expect(s3Mock.commandCalls(CreateMultipartUploadCommand).length).toBe(
            1
        );
        const partCalls = s3Mock.commandCalls(UploadPartCommand);
        expect(partCalls.length).toBe(Math.ceil(size / ctx.multipartThreshold));
        expect(s3Mock.commandCalls(CompleteMultipartUploadCommand).length).toBe(
            1
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
