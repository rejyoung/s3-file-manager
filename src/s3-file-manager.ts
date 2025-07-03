import { S3Client } from "@aws-sdk/client-s3";
import { FMConfig, Logger, WithSpanFn } from "./types/fmconfig-types";
import { isValidLogger } from "./utils/isValidLogger";

export class S3FileManager {
    private bucketName: string;
    private s3: S3Client;
    private logger: Logger;
    private withSpan: WithSpanFn;

    constructor(config: FMConfig) {
        this.bucketName = config.bucketName;

        this.s3 = new S3Client({
            region: config.bucketRegion,
            endpoint: config.endpoint ?? undefined,
            credentials: config.credentials,
            forcePathStyle: config.forcePathStyle,
        });

        this.logger = isValidLogger(config.logger)
            ? config.logger
            : { info: console.log, warn: console.warn, error: console.error };

        this.withSpan = config.tracer ?? (async (_n, _m, work) => await work());
    }
}
