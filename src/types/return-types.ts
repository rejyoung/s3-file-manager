export interface DeleteReturnType {
    success: boolean;
    deleted: boolean;
    filePath: string;
    reason?: string;
}

export interface CopyReturnType {
    success: boolean;
    source: string;
    destination: string;
}

export interface MoveReturnType {
    success: boolean;
    source: string;
    destination: string;
    originalDeleted: boolean;
}

export interface RenameReturnType {
    success: boolean;
    oldPath: string;
    newPath: string;
    originalDeleted: boolean;
}
