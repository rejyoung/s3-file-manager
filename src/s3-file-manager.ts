import { FMConfig } from "./types/fmconfig-types.js";
import {
    ConfirmFilesOptions,
    FilePayload,
    FileUploadOptions,
    ListFilesOptions,
} from "./types/input-types.js";
import { S3FMContext } from "./core/context.js";
import { UploadManager } from "./core/uploadManager.js";
import { FileService } from "./core/fileService.js";

export class S3FileManager {
    private readonly sharedContext: S3FMContext;
    private readonly uploads: UploadManager;
    private readonly services: FileService;

    constructor(config: FMConfig) {
        this.sharedContext = new S3FMContext(config);
        this.uploads = new UploadManager(
            this.sharedContext,
            config.maxUploadConcurrency
        );
        this.services = new FileService(this.sharedContext);
    }

    public async listFiles(options: ListFilesOptions = {}): Promise<string[]> {
        return this.services.listFiles(options);
    }

    public async confirmFilesExist(
        options: ConfirmFilesOptions
    ): Promise<{ allExist: boolean; missingFiles: string[] }> {
        return this.services.confirmFilesExist(options);
    }

    public async uploadFile(
        file: FilePayload,
        options: FileUploadOptions
    ): Promise<void> {
        return this.uploads.uploadFile(file, options);
    }
}
