module.exports = {
    /**
     * Async proxy to the ESM-only `file-type` API,
     * loaded via dynamic import in a CommonJS context.
     */
    fileTypeFromBuffer: async (buffer) => {
        const { fileTypeFromBuffer } = await import("file-type");
        return fileTypeFromBuffer(buffer);
    },
};
