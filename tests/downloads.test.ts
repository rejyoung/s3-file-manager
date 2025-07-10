import { describe, it, expect, vi, beforeEach } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { S3FMContext } from "../src/core/context";
import {
    GetObjectCommand,
    HeadObjectCommand,
    HeadObjectCommandOutput,
    S3Client,
} from "@aws-sdk/client-s3";
import { DownloadManager } from "../src/core/downloadManager";
import { PassThrough } from "stream";
import { IncomingMessage } from "http";
import { SdkStreamMixin } from "@aws-sdk/types";

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
} as unknown as S3FMContext;

describe("DownloadManager", () => {
    let downloads: DownloadManager;

    beforeEach(() => {
        s3Mock.reset();
        vi.clearAllMocks();
        downloads = new DownloadManager(mockCtx);
    });

    describe("streamFile", () => {
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

            const stream = await downloads.streamFile("hello/world.txt", {
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

    // describe("loadFile", () => {});

    // describe("downloadToDisk", () => {});

    // describe("downloadAllToDisk", () => {});

    describe("generateTempDownloadLink", () => {
        it("should return a presigned url with the expected expiration date", async () => {
            s3Mock;
        });
    });

    // describe("getFileMetadata", () => {});

    // describe("getFileFormat", () => {});
});
