import { describe, it, expect, vi, beforeEach } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { S3FMContext } from "../src/core/context";

describe("S3FMContext.listItems", () => {
    let s3Mock: ReturnType<typeof mockClient>;
    let ctx: S3FMContext;

    beforeEach(() => {
        // Create mock S3 client
        s3Mock = mockClient(S3Client);

        // Instantiate real context
        ctx = new S3FMContext({
            bucketName: "test-bucket",
            bucketRegion: "us-east-1",
        });

        // Inject the mock client
        (ctx as any).s3 = s3Mock;
    });

    it("should list files across multiple pages and sort them", async () => {
        // First page
        s3Mock
            .on(ListObjectsV2Command, {
                Bucket: "test-bucket",
                Prefix: "files/",
                Delimiter: undefined,
            })
            .resolvesOnce({
                Contents: [{ Key: "files/b.txt" }, { Key: "files/a.txt" }],
                NextContinuationToken: "token1",
            });
        // Second (final) page
        s3Mock
            .on(ListObjectsV2Command, {
                Bucket: "test-bucket",
                Prefix: "files/",
                Delimiter: undefined,
                ContinuationToken: "token1",
            })
            .resolvesOnce({
                Contents: [{ Key: "files/c.txt" }],
            });

        const result = await ctx.listItems("files", {});
        expect(result).toEqual(["files/a.txt", "files/b.txt", "files/c.txt"]);
    });

    it("should list only directory prefixes when directoriesOnly is true", async () => {
        s3Mock.on(ListObjectsV2Command).resolves({
            CommonPrefixes: [{ Prefix: "dir1/" }, { Prefix: "dir2/" }],
        });

        const result = await ctx.listItems("", { directoriesOnly: true });
        expect(result).toEqual(["dir1/", "dir2/"]);
    });

    it("should apply filterFn and compareFn correctly", async () => {
        s3Mock.on(ListObjectsV2Command).resolves({
            Contents: [
                { Key: "apricot.txt" },
                { Key: "apple.txt" },
                { Key: "banana.txt" },
                { Key: "cherry.txt" },
            ],
        });

        const filterFn = (name: string) => name.startsWith("a");
        const compareFn = (x: string, y: string) => x.localeCompare(y);

        const result = await ctx.listItems("", {
            filterFn,
            compareFn,
        });
        expect(result).toEqual(["apple.txt", "apricot.txt"]);
    });
});
