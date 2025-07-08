import { FileService } from "../src/core/fileService";
import { S3FMContext } from "../src/core/context";
import { mockClient } from "aws-sdk-client-mock";
import {
    S3Client,
    CopyObjectCommand,
    DeleteObjectCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
} from "@aws-sdk/client-s3";

const s3Mock = mockClient(S3Client);

const mockCtx = {
    bucketName: "test-bucket",
    s3: s3Mock,
    withSpan: async (_name: string, _attrs: any, fn: Function) => await fn(),
    verboseLog: jest.fn(),
    handleRetryErrorLogging: jest.fn(),
    logger: {
        info: jest.fn(),
        warn: jest.fn(),
    },
} as unknown as S3FMContext;

const fileService = new FileService(mockCtx);

beforeEach(() => {
    s3Mock.reset();
    jest.clearAllMocks();
});

describe("FileService", () => {
    describe("listFiles", () => {
        it("should return a list of file keys", async () => {
            s3Mock.on(ListObjectsV2Command).resolves({
                Contents: [{ Key: "file1.txt" }, { Key: "file2.txt" }],
            });

            const files = await fileService.listFiles();
            expect(files).toEqual(["file1.txt", "file2.txt"]);
        });
    });

    describe("listDirectories", () => {
        it("should return a list of directory prefixes", async () => {
            s3Mock.on(ListObjectsV2Command).resolves({
                CommonPrefixes: [{ Prefix: "dir1/" }, { Prefix: "dir2/" }],
            });

            const dirs = await fileService.listDirectories();
            expect(dirs).toEqual(["dir1/", "dir2/"]);
        });
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
