/**
 * Type guard to validate a logger implements the Logger interface.
 */

import { Logger } from "../types/fmconfig-types";

export const isValidLogger = (obj: any): obj is Logger => {
    if (
        obj != null &&
        typeof (obj as Logger).info === "function" &&
        typeof (obj as Logger).warn === "function" &&
        typeof (obj as Logger).error === "function"
    ) {
        return true;
    } else if (obj != null) {
        console.error(
            `Invalid \`logger\` provided (type: ${typeof obj}); ` +
                `it must have \`info\`, \`warn\`, and \`error\` methods. ` +
                `Using default console logger instead.`
        );
    }
    return false;
};
