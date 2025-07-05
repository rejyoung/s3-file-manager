const BASE_DELAY_MS = 100;

export const wait = async (ms: number): Promise<void> => {
    return new Promise((resolve) => setTimeout(resolve, ms));
};

export function backoffDelay(attempt: number, cap = 10_000): number {
    const exp = Math.min(cap, BASE_DELAY_MS * 2 ** (attempt - 1));
    // jitter between exp/2 and exp
    return Math.floor(exp / 2 + Math.random() * (exp / 2));
}
