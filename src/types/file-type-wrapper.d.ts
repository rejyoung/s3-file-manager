declare module "s3-file-manager/file-type-wrapper" {
    // Pull in both the implementation and the types from file-type
    export { fileTypeFromBuffer } from "file-type";
}
