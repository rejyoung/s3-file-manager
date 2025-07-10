import { describe, it, expect, vi, beforeEach } from "vitest";
import { FileService } from "../src/core/fileService";
import { S3FMContext } from "../src/core/context";
import { mockClient } from "aws-sdk-client-mock";
import {
    S3Client,
    CopyObjectCommand,
    DeleteObjectCommand,
    HeadObjectCommand,
} from "@aws-sdk/client-s3";

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

describe("FileService", () => {
    let fileService: FileService;

    beforeEach(() => {
        s3Mock.reset();
        vi.clearAllMocks();
        fileService = new FileService(mockCtx);
    });

    describe("confirmFilesExist", () => {
        it("should return an empty array if all files exist", async () => {
            s3Mock.on(HeadObjectCommand).resolves({});

            const missing = await fileService.confirmFilesExist([
                "file1.txt",
                "file2.txt",
            ]);
            expect(missing).toEqual([]);
        });

        it("should return missing files if not found", async () => {
            s3Mock.on(HeadObjectCommand).rejects({
                name: "NotFound",
                $metadata: { httpStatusCode: 404 },
            });

            const missing = await fileService.confirmFilesExist([
                "file1.txt",
                "file2.txt",
            ]);
            expect(missing).toEqual(["file1.txt", "file2.txt"]);
        });
    });

    describe("copyFile", () => {
        it("should call CopyObjectCommand and return success", async () => {
            s3Mock.on(CopyObjectCommand).resolves({});

            const result = await fileService.copyFile("file1.txt", "newdir/");
            expect(result.success).toBe(true);
            expect(result.source).toContain("file1.txt");
            expect(result.destination).toContain("newdir/");
        });
    });

    describe("deleteFile", () => {
        it("should delete a file and return success", async () => {
            s3Mock.on(DeleteObjectCommand).resolves({});

            const result = await fileService.deleteFile("file1.txt");
            expect(result.success).toBe(true);
            expect(result.deleted).toBe(true);
        });

        it("should handle file not found gracefully", async () => {
            s3Mock.on(DeleteObjectCommand).rejects({ name: "NoSuchKey" });

            const result = await fileService.deleteFile("missing.txt");
            expect(result.success).toBe(false);
            expect(result.deleted).toBe(false);
            expect(result.reason).toBe("File not found");
        });
    });
});
