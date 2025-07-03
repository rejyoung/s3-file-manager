import { S3FileManager } from "./s3-file-manager.js";
import { FMConfig } from "./types/fmconfig-types.js";

export default S3FileManager;
export { FMConfig };
export type { ListFilesOptions } from "./types/input-types.js";

export const createFileManager = (config: FMConfig) => {
    const fm = new S3FileManager(config);
    return {
        listFiles: fm.listFiles.bind(fm),
    };
};
