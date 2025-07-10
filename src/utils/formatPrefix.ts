export const formatPrefix = (filename: string, prefix: string): string => {
    let formattedPrefix: string;

    const prefixSlash = prefix.endsWith("/");
    const filenameSlash = filename.startsWith("/");

    if (prefixSlash && filenameSlash) {
        formattedPrefix = prefix.slice(0, -1);
    } else if (!prefixSlash && !filenameSlash) {
        formattedPrefix = prefix + "/";
    } else {
        formattedPrefix = prefix;
    }

    return formattedPrefix;
};
