
# S3-file-manager

## Table of Contents

- [1. Getting Started](#1-getting-started)
  - [Introduction](#introduction)
  - [Quickstart](#quickstart)
  - [Credential Setup](#credential-setup-required)
  - [Configuration Options](#configuration-options-fmconfig)
- [2. API Overview](#2-api-overview)
  - [Usage Notes](#usage-notes)
  - [File Service Methods](#file-service-methods)
  - [Upload Methods](#upload-methods)
  - [Download Methods](#download-methods)
- [3. Types](#3-types)
- [4. Logging and Tracing](#4-logging-and-tracing)
  - [Logger](#logger)
  - [Tracing Function (withSpan)](#tracing-function-withspan)
- [5. Automatic Retries and Error Handling](#5-automatic-retries-and-error-handling)
- [6. Running Tests](#6-running-tests)

## 1. Getting Started

### Introduction

S3 File Manager is a lightweight TypeScript utility that wraps the complexity of working with S3-style object storage into a simple, consistent API—no deep knowledge of the AWS SDK required.  Whether you’re new to cloud storage or an S3 power-user, you can now list and filter your files or “folders,” upload both small and large files with a single method call, download content as text, JSON, or raw data, and stream or save files to disk—all with built-in retry and performance optimizations.  Common tasks like copying, moving, renaming, and deleting single files or entire prefixes (“folders”) become one-line commands, and you can even generate time-limited download links without touching low-level presigner code.  By automatically choosing the right upload/download strategy and handling retries for you, it cuts dozens of lines of boilerplate into a few clear method calls—whether you’re pointed at Amazon S3 or any S3-compatible service (MinIO, Cloudflare R2, DigitalOcean Spaces, etc.).

### Quickstart

#### 1. Install the package

```sh
npm install s3-file-manager
```

> Requires Node.js 18+  

> Supports both ESM and CommonJS

#### 2. Create a file manager instance

```ts
import S3FileManager from "s3-file-manager"; // ESM
// or
const S3FileManager = require("s3-file-manager"); // CommonJS

const fm = new S3FileManager({
  bucketName: "my-bucket",
  bucketRegion: "us-east-1",
  credentials: {
    accessKeyId: YOUR_ACCESS_KEY,
    secretAccessKey: YOUR_SECRET_ACCESS_KEY
  }
});
```

You can now use `fm` (or whatever variable name you choose) to call any method, such as `fm.uploadFile()` or `fm.listFiles()`.

> S3-file-manager is a class-based library and must be instantiated before use. Calling `new S3FileManager()` and passing in a `config` object returns such an instance. (For details on the `config` object, see [Configuration Options](#configuration-options-fmconfig).)

> Because each instance is self-contained, you can potentially run multiple instances simultaneously, each configured for a different bucket or storage provider.

### Credential Setup (Required)

This library uses the [AWS SDK for JavaScript (v3)](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/index.html) under the hood. You must authenticate with your S3 or S3-compatible provider using one of the SDK’s supported methods. The easiest way to do this when using this library is to pass `credentials` directly in the config:

  ```ts
  {
    credentials: {
      accessKeyId: YOUR_ACCESS_KEY,
      secretAccessKey: YOUR_SECRET_ACCESS_KEY
    }
  }
  ```

See the [`credentials`](#credentials-optional) parameter definition in the configuration options for more details.

Alternatively, you can use one of the following methods:

- Set environment variables:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
- Use a shared credentials file (`~/.aws/credentials`)
- Use instance roles (e.g., EC2, ECS)
- Use any custom credential provider supported by the AWS SDK

For more details on supported credential sources and how to configure them, see the [AWS SDK v3 credential documentation](https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/setting-credentials-node.html)

#### 🔐 Required Permissions

Your credentials must allow the following S3 actions, depending on the methods you use:

- `s3:ListBucket` — For listing files, verifying existence, deleting folders.
- `s3:GetObject` — For downloading files, generating presigned URLs.
- `s3:PutObject` — For uploading or copying files.
- `s3:DeleteObject` — For deleting files or folders.

For methods supporting cross-bucket operations (e.g. `copyFile()`), you must have `GetObject` access on the **source** bucket and `PutObject` access on the **destination** bucket.

> **Note:** These permissions reflect AWS-style access control. Most S3-compatible providers (e.g. MinIO, Cloudflare R2, Wasabi) support similar actions, but exact behavior may vary. Refer to your provider’s documentation for details.

### Configuration Options (FMConfig)

A configuration object of the following type must be passed as an argument when calling `new S3FileManager()` to create a new file manager instance:

```ts
interface FMConfig {
    bucketName: string;
    bucketRegion: string;
    endpoint?: string;
    forcePathStyle?: boolean;
    credentials?: AwsCredentialIdentity | Provider<AwsCredentialIdentity>;
    maxAttempts?: number;
    maxUploadPartSizeMB?: number;
    maxUploadConcurrency?: number;
    logger?: Logger;
    verboseLogging?: boolean;
    withSpan?: WithSpanFn;
}
```

#### `bucketName` (required)

Name of the S3 bucket you're interacting with.

#### `bucketRegion` (required)

The region your bucket is hosted in. This value is always required by the AWS SDK that underpins this library, but for S3-compatible services (like MinIO, Cloudflare R2, or DigitalOcean Spaces), it may be:

- Arbitrary (e.g., "us-east-1" for MinIO)
- Ignored by the service if a custom endpoint is provided
- Required to match the region embedded in the endpoint (e.g., "nyc3" for DigitalOcean Spaces)

Even if your provider doesn’t use AWS regions, you must still supply a string to satisfy the SDK.

> 📘 Check your provider’s documentation to confirm whether a specific region value is needed.
If unsure, use "us-east-1" and set an appropriate endpoint. If unsure, use "us-east-1" and set a custom endpoint to route traffic correctly.

#### `endpoint` (optional)

A custom endpoint URL for your S3-compatible storage provider. This is required only if you’re using a non-AWS service (such as MinIO, Cloudflare R2, DigitalOcean Spaces, etc.) or an AWS-compatible proxy.

- For AWS S3, you typically do not need to set this — the SDK will infer the correct endpoint based on the region and bucket.
- For non-AWS providers, setting a custom endpoint is usually required. The format will typically look like `https://<region>.<provider>.com` or `http://localhost:9000` for local services like MinIO.
- If you provide an endpoint, the SDK will route all traffic there instead of using the AWS default

> 📘 For non-AWS services, check your provider’s documentation to determine whether a custom endpoint is required, optional, or ignored.

Examples:

```ts
// DigitalOcean Spaces
endpoint: "https://nyc3.digitaloceanspaces.com"

// MinIO (local)
endpoint: "http://localhost:9000"

// Cloudflare R2
endpoint: "https://<account>.r2.cloudflarestorage.com"
```

#### `forcePathStyle` (optional)

Specifies whether to use path-style URLs (e.g., `https://host/bucket/key`) instead of virtual-hosted-style (`https://bucket.host/key`).
This is **required for MinIO** and some local S3 emulators. For AWS S3, you typically do not need to set this—by default, the SDK will use the most efficient style automatically based on the endpoint and region.

> 📘 Check your provider’s documentation to determine whether forcePathStyle must be set for your environment.

#### `credentials` (optional)

AWS credentials used to authenticate with the S3-compatible service. This can either be:

- A static credentials object {`accessKeyId`, `secretAccessKey`}
- A provider function that resolves to such an object (e.g., from @aws-sdk/credential-provider-node or a custom async source)

If not supplied, the AWS SDK's default credential resolution chain will be used. That includes environment variables, shared credentials files, EC2/ECS instance roles, etc.

#### `maxAttempts` (optional)

The maximum number of total attempts the library will make to execute a failed S3 operation, including the initial attempt and retries. If omitted, the library uses the default maximum of 3.

#### `maxUploadPartSizeMB` (optional)

The maximum size (in megabytes) of a chunk of upload data. Files larger than this will be automatically uploaded in multiple parts. Must be a number greater than 5 and less than 100. If omitted or if the number provided exceeds the specified limits, the library uses the default maximum upload part size of 10MB.

#### `maxUploadConcurrency` (optional)

The maximum number of simultaneous upload operations. This includes both entire files (for small uploads) or individual chunks (for multipart uploads). Upload operations beyond this limit are automatically queued and processed as slots become available. This does not limit the size or number of files a user can attempt to upload at once. If omitted, the library uses the default maximum of 4.

#### `logger` (optional)

A custom logging utility that overrides the default console logger. Must implement info(), warn(), and error() methods. See [Logging and Tracing](#4-logging-and-tracing) for full details and usage.

#### `verboseLogging` (optional)

Enables detailed logging of S3 operations, including request attempts, retries, and internal upload behavior. Useful for debugging or monitoring upload activity. Defaults to false.

#### `withSpan` (optional)

A custom tracing function that wraps internal operations in named spans for observability. Must match the signature shown in [Logging and Tracing](#4-logging-and-tracing), where full usage details are provided.

## 2. API Overview

| Method                      | Description                                                | Args                                  | Returns                              |
| --------------------------- | ---------------------------------------------------------- | ------------------------------------- | ------------------------------------ |
| `listFiles()`               | List all file paths within a given folder                  | prefix, options?                      | Promise\<string[]>                   |
| `listFolders()`             | List all folders (prefixes) within a given folder          | prefix, options?                      | Promise\<string[]>                   |
| `verifyFilesExist()`        | Verify that all supplied file paths are valid              | filenames, options?                   | Promise\<string[]>                   |
| `copyFile()`                | Copy a file to a new location                              | filePath, destinationPrefix, options? | Promise\<CopyReturnType>             |
| `moveFile()`                | Move a file to a new location                              | filePath, destinationPrefix, options? | Promise\<MoveReturnType>             |
| `renameFile()`              | Rename a file                                              | filePath, newFilename, options?       | Promise\<RenameReturnType>           |
| `deleteFile()`              | Delete a file from the bucket                              | filePath, options?                    | Promise\<DeleteReturnType>           |
| `deleteFolder()`            | Delete all files in a given folder                         | prefix, options?                      | Promise\<DeleteFolderReturnType>     |
| `uploadFile()`              | Upload a file to the bucket                                | file, options?                        | Promise\<string>                     |
| `uploadMultipleFiles()`     | Upload multiple files to the bucket                        | files, options?                       | Promise\<string[]>                   |
| `uploadFromDisk()`          | Upload a file on local disk to the bucket                  | filePath, options?                    | Promise\<string>                     |
| `uploadMultipleFromDisk()`  | Upload multiple files on local disk to the bucket          | filePaths, options?                   | Promise\<string[]>                   |
| `getStream()`               | Fetch a readable stream of a file                          | filePath, options?                    | Promise\<Readable>                   |
| `downloadFile()`            | Download a file to memory                                  | filePath, options?                    | Promise\<Buffer \| string \| object> |
| `downloadToDisk()`          | Download a file to the disk                                | filePath, outDir, options?            | Promise\<void>                       |
| `downloadFolderToDisk()`    | Download the entire contents of a folder to the disk       | prefix, outDir, options?              | Promise\<DownloadFolderReturnType>   |
| `getTemporaryDownloadUrl()` | Generate an expiring download link                         | filePath, options?                    | Promise\<string>                     |

### Usage Notes

All public methods of an `S3FileManager` instance accept an optional final argument: an `options` object.

- This object typically includes settings specific to the particular method, like `prefix`, `contentType` or `outDir`.
- **If tracing is enabled** (via `withSpan`), all options objects will also include an optional `spanOptions` field for span metadata. (See [Logging and Tracing](#4-logging-and-tracing) for more information.)
- You can omit the `options` object entirely if you don't need to pass any options.

#### Note on Prefixes and "Folders"

S3 and S3-compatible services use a flat key namespace. What appear to be folders are actually just key prefixes—string segments that group related objects.

Throughout this library:

- The term **prefix** refers to the key path portion of the object name (e.g., `"photos/2023/"`).
- The term **"folder"** is used informally to refer to a group of objects with a shared prefix.

> For example, the object key `photos/2023/image1.jpg` is considered part of the `"photos/2023/"` folder.

Methods like `listFolders`, `deleteFolder`, and `downloadFolderToDisk` operate on these prefix-based groupings.

See [AWS: Organizing objects using prefixes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html) for more background.

### File Service Methods

#### `listFiles()`

```ts
const files = await fm.listFiles(
    "media-files/img/", 
    {
        filterFn: (filePath) => filePath.endsWith(".jpg")
        compareFn: (a, b) => a.localeCompare(b)
    }
) 
// ["media-files/img/headshot-1.jpg", "media-files/img/headshot-2.jpg", "media-files/img/user-uploads/img_23049.jpg"]
```

**Arguments:**

- `prefix` - The path of the folder to be checked. To list all files contained in the bucket, pass an empty string (`""`).
- `options` *(optional)* - An object of type `ListFilesOptions`. May include:
  - `filterFn` *(optional)* - A function to exclude file paths from the result (applied using `results.filter(filterFn)`).
  - `compareFn` *(optional)* - A custom sorting function for result order (applied using `results.sort(compareFn)`).
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**

A promise resolving to an array of strings representing all the files found within the specified folder. The returned paths include the full prefix and any nested folder structure — not just the filenames.

#### `listFolders()`

```ts
const folders = await fm.listFolders(
    "media-files/"
    {
        filterFn: (folderPath) => !folderPath.includes("user-")
        compareFn: (a, b) => {
            const depthA = a.split("/").length;
            const depthB = b.split("/").length;
            return depthA - depthB;
        }
    }
) 
// ["media-files/shared/", "media-files/img/", "media-files/img/web/", "media-files/shared/archive/2023/"]
```

**Arguments:**

- `prefix` - The path of the folder to be checked. To list all folders contained in the bucket, pass an empty string (`""`).
- `options` *(optional)* - An object of type `ListFoldersOptions`. May include:
  - `filterFn` *(optional)* - A function to exclude folder paths from the result (applied using `results.filter(filterFn)`).
  - `compareFn` *(optional)* - A custom sorting function for result order (applied using `results.sort(compareFn)`).
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**

An promise resolving to an array of strings representing all the folders found within the specified folder. The returned paths include the full prefix and any nested folder structure — not just the folder names.

#### `verifyFilesExist()`

```ts
const missing = await fm.verifyFilesExist(
    ["img/bridge.webp", "user-img/headshot.jpg"], 
    {prefix: "media-files/"}
) // ["media-files/img/bridge.webp"]
```

**Arguments:**

- `filePaths` - An array of full file paths to be checked.
- `options` *(optional)* - An object of type `VerifyFilesOptions`. May include:
  - `prefix` *(optional)* - A string shared by all submitted paths, used to avoid repeating the prefix in each file name.
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**  

A promise resolving to an array of file paths that were missing from the bucket. An empty array indicates that all submitted paths were valid.

#### `copyFile()`

```ts
const result = await fm.copyFile(
    "img/headshot.jpg", 
    "media-files/user-img/",
    {
        sourceBucketName: "Uploads"
    }
)
/*
{
  success: true,
  source: "uploads/img/headshot.jpg",
  destination: "media-files/user-img/headshot.jpg"
}
*/
```

Calling `copyFile()` creates a copy of a file at the specified location.

**Arguments:**

- `filePath` – Full path of the file to move (e.g. `"img/headshot.jpg"`).
- `destinationFolder` – Folder to move the file into (must end in `/` and be located in the default bucket).
- `options` *(optional)* - An object of type `CopyFileOptions`. May include:
  - `sourceBucketName` *(optional)* - The name of the source bucket (if different from the default bucket configured for this instance).
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**  

A promise resolving to an object of type `CopyReturnType` with:

- `success` – Whether the copy succeeded.
- `source` – Full path of the original file.
- `destination` – Full path of the copied file.

#### `moveFile()`

```ts
const result = await fm.moveFile(
  "img/headshot.jpg", 
  "media-files/user-img/",
  { sourceBucketName: "uploads" }
)
/*
{
  success: true,
  source: "uploads/img/headshot.jpg",
  destination: "media-files/user-img/headshot.jpg",
  originalDeleted: true
}
*/
```

> ⚠️ Requires `s3:DeleteObject` on the source bucket. See [required permissions](#-required-permissions)

Moves a file from one location to another — optionally across buckets. This is done by copying the file to the new location and then deleting the original.

**Arguments:**

- `filePath` – Full path of the file to move (e.g. `"img/headshot.jpg"`).
- `destinationFolder` – Folder to move the file into (must end in `/` and be located in the default bucket).
- `options` *(optional)* – An object of type `MoveFileOptions`. May include:
  - `sourceBucket` *(optional)* – Name of the source bucket (if different from the default).
  - `spanOptions` *(optional)* – Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**  

A promise resolving to an object of type `MoveReturnType` with:

- `success` – Whether the copy succeeded.
- `source` – Full path of the original file.
- `destination` – Full path of the copied file.
- `originalDeleted` – `true` if the original file was deleted successfully.

If deletion fails, the method does not throw—the file will exist in both locations, and `originalDeleted` will be `false`.

#### `renameFile()`

```ts
const result = await fm.renameFile(
    "media-files/user-img/headshot.jpg", 
    "user1029HS.jpg",
)
/*
{
  success: true,
  source: "media-files/user-img/headshot.jpg",
  destination: "media-files/user-img/user1029HS.jpg",
  originalDeleted: true
}
*/
```

> ⚠️ Requires `s3:DeleteObject`. See [required permissions](#-required-permissions)

Calling `renameFile()` creates a new copy of the specified file with the provided filename in the same folder and then deletes the original file.

**Arguments:**

- `filePath` – Full path of the file to be renamed (e.g. `"media-files/user-img/headshot.jpg"`).
- `newFilename` - New name to be applied to the specified file.
- `options` *(optional)* - An object of type `RenameFileOptions`. May include:
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**  

A promise resolving to an object of type `RenameReturnType` with:

- `success` – Whether the copy succeeded.
- `source` – Full path of the file with its original filename.
- `destination` – Full path of the file with its new filename.
- `originalDeleted` – `true` if the original file was deleted successfully.

If deletion fails, the method does not throw—the file will exist under both names, and originalDeleted will be false.

#### `deleteFile()`

```ts
await fm.deleteFile("media-files/user-img/headshot.jpg")
```

Calling `deleteFile()` removes the file at the specified file path from the bucket.

**Arguments:**

- `filePath` – Full path of the file to be deleted (e.g. `"media-files/user-img/headshot.jpg"`).
- `options` *(optional)* - An object of type `DeleteFileOptions`. May include:
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**

A promise that resolves when the operation is complete

> If a file does not exist at the specified file path, the method will not throw an error. It will, however, log a warning if verbose logging is enabled.

#### `deleteFolder()`

```ts
const result = await fm.deleteFolder("media-files/user-img/")
/*
{
  success: true,
  message: "Folder media-files/user-img/ and the 12 files contained in it successfully deleted.",
  failed: 0
  succeeded: 12
  fileDeletionErrors: []
}
*/
```

Calling `deleteFolder()` removes all files within the specified folder.

**Arguments:**

- `folderPath` – Full path of the folder whose contents are to be deleted (e.g. `"media-files/user-img/"`).
- `options` *(optional)* - An object of type `DeleteFolderOptions`. May include:
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**

A promise resolving to an object of type `DeleteFolderReturnType` with:

- `success` - Whether the operation succeeded (either partially or completely).
- `message` - String providing further details about the results of the operation.
- `failed` - The number of deletions that failed.
- `succeeded` - The number of deletions that succeeded.
- `fileDeletionErrors` - An array of objects corresponding to each failure. Objects include:
  - The `filepath` of the file that could not be deleted.
  - The full error object for this file.

> If the deletion of any one file fails, the method will not throw an error. Errors are instead reported through the fileDeletionErrors array. If the folder does not exist (i.e. no files had the specified prefix), the function will report success and explain the situation in the return object message

### Upload Methods

#### `uploadFile()`

```ts
import fs from "fs/promises";

const fileBuffer = await fs.readFile("img_23049.jpg")

await fm.uploadFile(
    {
        name: "headshot.jpg",
        content: fileBuffer, // Buffer
        contentType: "image/jpeg", // mime-type
        sizeHintBytes: 69632
    },
    {
        prefix: "media-files/user-img/
    }
    // "media-files/user-img/headshot.jpg"
)
```

**Arguments:**

- `file` - A `FilePayload` object the includes:
  - `name` - The name to be assigned to the file.
  - `content` - The file data to upload. Supports multiple types, including:
    - `string` - interpreted as UTF-8 text
    - `Buffer`/`Uint8Array` - for binary data
    - `Blob` - typically used in browser environments
    - `Readable` - Node.js readable streams
    - `ReadableStream` - web streams (e.g. from `fetch().body`)
  - `contentType` *(optional)* - MIME type of the file.
  - `sizeHintBytes` *(optional)* -  Expected size of the file. Providing this value avoids the need to calculate the size programmatically, improving performance.
- `options` *(optional)* - An object of type `UploadOptions`. May include:
  - `prefix` *(optional)* - Path to the folder (prefix) where the file should be added. If omitted, the file will be uploaded to the root of the storage bucket.
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**

A promise resolving to a string containing the full path of the newly uploaded file.

#### `uploadMultipleFiles()`

```ts
// Example: Uploading a string, a user-selected file (Blob), and a streamed remote file
const textFile = {
  name: "notes.txt",
  content: "This is a plain text file.",
  contentType: "text/plain",
  sizeHintBytes: Buffer.byteLength("This is a plain text file.", "utf-8")
};

const fileInput = document.querySelector("input[type='file']");
const selectedFile = fileInput.files[0]; // Blob from <input>
const blobFile = {
  name: selectedFile.name, // sample.mov
  content: selectedFile,
  contentType: selectedFile.type,
  sizeHintBytes: selectedFile.size
};

const response = await fetch("https://example.com/data.json");
const streamFile = {
  name: "remote-data.json",
  content: response.body, // ReadableStream
  contentType: response.headers.get("content-type") ?? "application/json",
  sizeHintBytes: parseInt(response.headers.get("content-length") || "0", 10)
};

await fm.uploadMultipleFiles([textFile, blobFile, streamFile], {
  prefix: "uploads/mixed/"
});
/*
    {
        filePaths: ["uploads/mixed/notes.txt", "uploads/mixed/sample.mov", "uploads/mixed/remote-data.json"]
        skippedFiles: []
    }
*/
```

**Arguments:**

- `files` - An array of `FilePayload` objects, each of which includes:
  - `name` - The name to be assigned to the file.
  - `content` - The file data to upload. Supports multiple types, including:
    - `string` - interpreted as UTF-8 text
    - `Buffer`/`Uint8Array` - for binary data
    - `Blob` - typically used in browser environments
    - `Readable` - Node.js readable streams
    - `ReadableStream` - web streams (e.g. from `fetch().body`)
  - `contentType` *(optional)* - MIME type of the file.
  - `sizeHintBytes` *(optional)* -  Expected size of the file. Providing this value avoids the need to calculate the size programmatically, improving performance.
- `options` *(optional)* - An object of type `UploadOptions`. May include:
  - `prefix` *(optional)* - Path to the folder (prefix) where the files should be added. If omitted, files will be uploaded to the root of the storage bucket.
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**
A promise resolving to an object of type `UploadFilesReturnType` with:

- `filePaths` - An array of strings representing the complete file paths for each successfully uploaded file.
- `skippedFiles` - An array containing the names of all files whose uploads failed.

> If a file fails to upload, the method will not throw an error; instead, it returns the names of all files that failed.

#### `uploadFromDisk()`

```ts
await fm.uploadFromDisk(
    "/Users/John/Downloads/headshot.jpg",
    {
        prefix: "media-files/user-img/
    }
    // "media-files/user-img/headshot.jpg"
)
```

**Arguments:**

- `filePath` - The local file path to be uploaded
- `options` *(optional)* - An object of type `UploadOptions`. May include:
  - `prefix` *(optional)* - Path to the folder (prefix) where the file should be added. If omitted, the file will be uploaded to the root of the storage bucket.
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**

A promise resolving to a string containing the full path of the newly uploaded file.

> This method reads a local file from disk and automatically constructs the appropriate file buffer or readable stream based on its size, preparing it for upload to the storage bucket.

#### `uploadMultipleFromDisk()`

```ts
await fm.uploadMultipleFromDisk(
    [
    "/Users/John/Downloads/headshot.jpg", 
    "/Users/John/Documents/resume.pdf", 
    "/Users/John/Documents/cover-letter.pdf"
    ], 
    {
        prefix: "uploads/mixed/"
    }
);
/*
    {
        filePaths: ["uploads/mixed/headshot.jpg", "uploads/mixed/resume.pdf", "uploads/mixed/cover-letter.pdf"]
        skippedFiles: []
    }
*/
```

**Arguments:**

- `filesPaths` - An array of local file paths to be uploaded.
- `options` *(optional)* - An object of type `UploadOptions`. May include:
  - `prefix` *(optional)* - Path to the folder (prefix) where the files should be added. If omitted, files will be uploaded to the root of the storage bucket.
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**
A promise resolving to an object of type `UploadFilesReturnType` with:

- `filePaths` - An array of strings representing the complete file paths for each successfully uploaded file.
- `skippedFiles` - An array containing the names of all files whose uploads failed.

> This method reads local files from disk and automatically constructs the appropriate file buffer or readable stream for each based on its size, preparing it for upload to the storage bucket. If a file fails to upload, the method will not throw an error; instead, it returns the names of all files that failed.

### Download Methods

#### `getStream()`

```ts
const stream = await fm.getStream(
    "data-dumps/user-activity-10-23-2025.csv"
    {
        timeoutMS: 20000
    }
)
// returns a Readable stream
```

**Arguments:**

- `filePath` - The full path to a file in the default S3 bucket.
- `options` *(optional)* - An object of type `GetStreamOptions`. May include:
  - `timeoutMS` *(optional)* -  Maximum time (in milliseconds) to wait for a response from S3 when requesting the file stream. If the stream fails to begin within this time, the request is automatically aborted. Defaults to 10000 (10 seconds).
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**

A promise resolving to a Readable stream.

#### `downloadFile()`

```ts
const file = await fm.downloadFile("uploads/mixed/remote-data.json")
/*
    {
        name: data
        location: data
        ... 
    }
*/
```

**Arguments:**

- `filePath` - The full path to a file in the default S3 bucket.
- `options` *(optional)* - An object of type `DownloadFileOptions`. May include:
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**

The return type depends on the file’s content-type:

- application/json → parsed object
- text/* → string
- everything else → Buffer

#### `downloadToDisk()`

```ts
await fm.downloadToDisk(
    "media-files/user-images/headshot.jpg", 
    "C:\\Users\\John\\Documents\\",
    {
        outputFilename: "my-headshot.jpg"
    }
)
```

**Arguments:**

- `filePath` - The full path of the file in the default storage bucket to be downloaded.
- `outDir` - The path on the local disk where the file will be saved.
- `options` *(optional)* - An object of type `DownloadToDiskOptions`. May include:
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**

A promise that resolves once the download is complete.

#### `downloadFolderToDisk()`

```ts
const result = await fm.downloadFolderToDisk(
    "media-files/user-images/"
    "/Users/user/Documents/",
    {
        extensionOverride: ".jpeg"
    }
)
/*
    {
        success: true,
        message: "All files with prefix media-files/user-images successfully downloaded.
        downloadedFiles: 12
        failedToDownload: []
    }
*/
```

**Arguments:**

- `prefix` - The full path of the folder in the default storage bucket to be downloaded.
- `outDir` - The path on the local disk where the folder will be saved.
- `options` *(optional)* - An object of type `DownloadFolderOptions`. May include:
  - `extensionOverride` *(optional)* - A file extension to apply to all downloaded files, replacing their original extensions.
  - `spanOptions` *(optional)* - Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**

An object with:

- `success` - Whether at least some of the files were successfully downloaded.
- `message` - A human-readable summary of the result.
- `downloadedFiles` - The number of files successfully downloaded.
- `failedToDownload` - An array containing the file paths that failed to download.

> If any file with the given prefix fails to download, the function **will not** throw an error. It will report the failure in the `failedToDownload` array. Error details can be viewed by turning on verbose logging.

> **NOTE:** The entire folder structure under the specified prefix will be replicated locally. For instance, the call made in the example above will download all files with that prefix to "/Users/user/Documents/user-images/". It will also preserve any nested structure, so that if "media-files/user-images/NorthAmerica/photo.jpg" exists, it will be saved to "/Users/user/Documents/user-images/NorthAmerica/photo.jpg".

#### `getTemporaryDownloadUrl()`

```ts
const url = await fm.getTemporaryDownloadUrl(
    "media-files/user-images/headshot.jpg",
    {
        expiresInSec: 300
    }
)
// https://your-bucket.s3.amazonaws.com/media-files/user-images/headshot.jpg?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...&X-Amz-Date=...&X-Amz-Expires=300&X-Amz-Signature=...
```

**Arguments:**

- `filePath` - The full path of the file in the default storage bucket for which the download URL should be generated.
- `options` *(optional)* - An object of type `GetDownloadUrlOptions`. May include:
  - `expiresInSec` *(optional)* - The length of time in seconds that the URL will remain valid. Defaults to 3600 (1 hr) if not provided.
  - `spanOptions` *(optional)*- Tracing options (used only with `withSpan`; [learn more](#4-logging-and-tracing)).

**Returns:**

An promise resolving to a pre-signed, expiring download URL.

> This URL can be used to download the file directly from your storage provider without requiring credentials, but will become invalid after the specified expiration time.

## 3. Types

All public-facing types used across this library are exported and can be imported as needed.

Most types are already described inline in the method documentation. This section lists the most relevant type names for reference.

Common Input Types

- `FilePayload` – Used when uploading a single file or array of files.
- `UploadOptions` – Options object for upload methods.
- `DownloadToDiskOptions`, `DownloadFileOptions`, `GetStreamOptions` – Options for download-related methods.
- `ListFilesOptions`, `ListFoldersOptions`, `VerifyFilesOptions` – Used for listing and verification.
- `CopyFileOptions`, `MoveFileOptions`, `RenameFileOptions`, `DeleteFileOptions`, `DeleteFolderOptions` – Options for file/folder operations.
- `GetDownloadUrlOptions` – Options for getTemporaryDownloadUrl.

Common Return Types

- `UploadFilesReturnType` – Returned by uploadMultipleFiles().
- `DownloadFolderReturnType` – Returned by downloadFolderToDisk().
- `CopyReturnType`, `MoveReturnType`, `RenameReturnType`, `DeleteFolderReturnType` – Returned by file/folder operation methods.

You can import these from the package root:

```ts
import type { UploadFilesReturnType, FilePayload } from "s3-file-manager";
```

## 4. Logging and Tracing

This library can be used with most structured logging libraries (e.g. pino, winston, bunyan) and distributed tracing tools (e.g. OpenTelemetry)—as well as any custom implementation of the same.

Both logging and tracing are configured via the S3FileManager constructor by supplying a `logger` or `withSpan` function in the config object.

### Logger

You may pass a custom logger to replace the default console methods. The logger must implement the following interface:

```ts
interface Logger {
  info(message: string, meta?: any): void;
  warn(message: string, meta?: any): void;
  error(message: string, meta?: any): void;
}
```

If no logger is provided, the library defaults to using console.log, console.warn, and console.error.

Custom loggers are useful for:

- Structured log output (e.g. JSON)
- Centralized logging pipelines (e.g., CloudWatch, Datadog, Logstash)
- Filtering or tagging logs by severity or source

### Tracing Function (withSpan)

You can enable distributed tracing by providing a withSpan function in the config. This function wraps critical internal operations (e.g. uploads, downloads, list operations) inside named “spans” for observability.

The function must match the following signature:

```ts
(name: string, attributes: Record<string, any>, fn: () => Promise<T>) => Promise<T>
```

If no tracer is provided, a no-op function is used internally.

This integration allows you to:

- Capture timing data for each operation
- Track high-level flow across multiple systems
- Add context tags/attributes to spans for filtering or analysis

#### Custom Span Metadata

Every method in the library automatically generates a span with default name and attributes when tracing is enabled. You can override these defaults by providing an object of the following type as the value of the `spanOptions` parameter inside the `options` argument when calling each method:

```ts
interface SpanOptions {
    name: string,
    attributes: Record<string, any>
}
```

For example:

```ts
await fm.uploadFile(file, {
  prefix: "media-files/",
  spanOptions: {
    name: "upload:user-headshot",
    attributes: { userId: "123", fileType: "image/jpeg" }
  }
});
```

## 5. Automatic Retries and Error Handling

One of the advantages of this library is its built-in retry logic. Every S3 operation is wrapped in a retry loop that continues until the operation succeeds or the maximum number of attempts is reached. By default, the library will try to run each operation **up to 3 times**, but this can be adjusted by setting the `maxAttempts` parameter in the [config object](#configuration-options-fmconfig).

Single-file operations (e.g. `uploadFile`, `renameFile`) will throw an error if all retry attempts fail. Multi-file operations (e.g. `uploadMultipleFiles`, `downloadFolderToDisk`) support partial success and will instead return a result object indicating which files (if any) failed without throwing an error.

If verbose logging is enabled, the library will emit a warning with full error details on each failed attempt—except the final attempt of single-file operations, where an error is thrown instead of a warning.

All warnings are routed through the configured logger—either a [custom logging utility](#4-logging-and-tracing) provided via the logger config parameter, or the default console.warn if none is supplied.

## 6. Running Tests

This project uses [Vitest](https://vitest.dev) for testing.

To run the tests:

```sh
npm test
```

> 🧪 Note: All tests are fully mocked using Vitest. No actual connection is made to an S3 service, so running the tests does not require AWS credentials or an internet connection.
