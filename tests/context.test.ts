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
                Prefix: undefined,
                Delimiter: undefined,
            })
            .resolvesOnce({
                Contents: [{ Key: "b.txt" }, { Key: "a.txt" }],
                NextContinuationToken: "token1",
            });
        // Second (final) page
        s3Mock
            .on(ListObjectsV2Command, {
                Bucket: "test-bucket",
                Prefix: undefined,
                Delimiter: undefined,
                ContinuationToken: "token1",
            })
            .resolvesOnce({
                Contents: [{ Key: "c.txt" }],
            });

        const result = await ctx.listItems({});
        expect(result).toEqual(["a.txt", "b.txt", "c.txt"]);
    });

    it("should list only directory prefixes when directoriesOnly is true", async () => {
        s3Mock.on(ListObjectsV2Command).resolves({
            CommonPrefixes: [{ Prefix: "dir1/" }, { Prefix: "dir2/" }],
        });

        const result = await ctx.listItems({ directoriesOnly: true });
        expect(result).toEqual(["dir1/", "dir2/"]);
    });

    it("should apply filterFn and compareFn correctly", async () => {
        s3Mock.on(ListObjectsV2Command).resolves({
            Contents: [
                { Key: "apple.txt" },
                { Key: "banana.txt" },
                { Key: "cherry.txt" },
            ],
        });

        const filterFn = (name: string) => name.startsWith("b");
        const compareFn = (x: string, y: string) => y.localeCompare(x);

        const result = await ctx.listItems({
            filterFn,
            compareFn,
        });
        expect(result).toEqual(["banana.txt"]);
    });
});
