/**
 * Typed React hooks for ws-asyncapi.
 *
 * `createReactClient<typeof channel>(url, path)` returns a set of hooks inferred
 * straight from the server `Channel` (no codegen) and bound to one connection.
 * The design **bridges to TanStack Query** instead of building a second cache:
 * RPCs become queries/mutations, and server events/streams patch the cache so
 * data stays live. Presence and streams use plain React state.
 *
 * ```tsx
 * import { createReactClient } from "@ws-asyncapi/react";
 * import type { chat } from "./server";
 *
 * export const ws = createReactClient<typeof chat>("ws://localhost:3000", "/chat/1");
 * // inside a component (under a TanStack QueryClientProvider):
 * const { data } = ws.useHistory("room:42", { liveEvent: "message", limit: 50 });
 * const { members, set } = ws.usePresence();
 * const send = ws.useMutate("sendMessage");
 * ```
 */
import {
    type QueryClient,
    useMutation,
    type UseMutationOptions,
    type UseMutationResult,
    useQuery,
    useQueryClient,
    type UseQueryOptions,
    type UseQueryResult,
} from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import {
    createClient,
    type HistoryEntry,
    RpcError,
    type TypedRpcError,
    type WebsocketAsyncAPIOptions,
} from "@ws-asyncapi/client";
import type { AnyChannel, InferClient } from "ws-asyncapi";

// --- stream fold (pure, testable; no React) ----------------------------------

/** How a stream's yielded items are reduced into the hook's `data`. */
export type StreamReduce<Item, Acc> =
    | { reduce: "append"; max?: number }
    | { reduce: (acc: Acc, item: Item) => Acc; initial: Acc };

/** Build the `{ initial, step }` fold for a {@link useStream} call. Default
 *  (no options) keeps only the **latest** item — O(1), nothing accumulated. */
export function streamFold<Item, Acc>(
    options?: StreamReduce<Item, Acc>,
): { initial: unknown; step: (acc: unknown, item: Item) => unknown } {
    if (!options)
        return { initial: undefined, step: (_acc, item) => item };
    if (options.reduce === "append") {
        const max = options.max;
        return {
            initial: [] as Item[],
            step: (acc, item) => {
                const next = [...(acc as Item[]), item];
                return max != null && next.length > max
                    ? next.slice(-max)
                    : next;
            },
        };
    }
    const { reduce, initial } = options;
    return {
        initial,
        step: (acc, item) => reduce(acc as Acc, item),
    };
}

// --- typed hook surface ------------------------------------------------------

/** The shape inferred from a channel — what the hooks are typed against. */
type Shape = InferClient<AnyChannel>;

/** Result of {@link ReactClient.useStream}. */
export interface StreamResult<Data> {
    /** the folded value (latest item by default; a list/aggregate with `reduce`) */
    data: Data;
    /** the stream completed normally */
    isDone: boolean;
    /** the stream failed (server error) */
    error: RpcError | null;
}

/** Live presence surface returned by {@link ReactClient.usePresence}. */
export interface PresenceResult<State> {
    members: Map<string, State>;
    self: string | null;
    set: (state: State) => Promise<void>;
    clear: () => Promise<void>;
}

export interface ReactClient<T extends Shape> {
    /** the underlying client (escape hatch: `opened`, raw `request`, etc.) */
    client: ReturnType<typeof createClient<AnyChannel>>;

    /** An RPC as a TanStack **query** (read). Keyed by `[path, "rpc", name, input]`. */
    useRequest<C extends keyof T["rpcMap"]>(
        command: C,
        input: T["rpcMap"][C]["input"],
        options?: Omit<
            UseQueryOptions<
                T["rpcMap"][C]["output"],
                TypedRpcError<T["rpcMap"][C]["errors"]>
            >,
            "queryKey" | "queryFn"
        >,
    ): UseQueryResult<
        T["rpcMap"][C]["output"],
        TypedRpcError<T["rpcMap"][C]["errors"]>
    >;

    /** An RPC as a TanStack **mutation** (write). `error` is the typed RpcError. */
    useMutate<C extends keyof T["rpcMap"]>(
        command: C,
        options?: Omit<
            UseMutationOptions<
                T["rpcMap"][C]["output"],
                TypedRpcError<T["rpcMap"][C]["errors"]>,
                T["rpcMap"][C]["input"]
            >,
            "mutationFn"
        >,
    ): UseMutationResult<
        T["rpcMap"][C]["output"],
        TypedRpcError<T["rpcMap"][C]["errors"]>,
        T["rpcMap"][C]["input"]
    >;

    /** Live presence roster + your own state. Re-renders on every diff. */
    usePresence(): PresenceResult<T["presenceState"]>;

    /**
     * A room's history as a query, optionally kept live by appending incoming
     * `liveEvent` events into the same cache (bounded by `limit`).
     */
    useHistory(
        room: string,
        options?: { liveEvent?: keyof T["eventMap"] & string; limit?: number },
    ): UseQueryResult<HistoryEntry<T["eventMap"]>[], RpcError>;

    /** Consume a stream. Default keeps the **latest** value (O(1)); pass
     *  `reduce` to accumulate. Auto-cancels (StreamStop) on unmount. */
    useStream<N extends keyof T["streamMap"]>(
        name: N,
        input: T["streamMap"][N]["input"],
    ): StreamResult<T["streamMap"][N]["output"] | undefined>;
    useStream<N extends keyof T["streamMap"]>(
        name: N,
        input: T["streamMap"][N]["input"],
        options: { reduce: "append"; max?: number },
    ): StreamResult<T["streamMap"][N]["output"][]>;
    useStream<N extends keyof T["streamMap"], Acc>(
        name: N,
        input: T["streamMap"][N]["input"],
        options: {
            reduce: (acc: Acc, item: T["streamMap"][N]["output"]) => Acc;
            initial: Acc;
        },
    ): StreamResult<Acc>;

    /** The most recent value of an event (stores one value, re-renders on each). */
    useLastEvent<E extends keyof T["eventMap"]>(
        event: E,
    ): T["eventMap"][E] | undefined;

    /** Run a side effect on each event (e.g. patch the cache). Stores nothing. */
    useEvent<E extends keyof T["eventMap"]>(
        event: E,
        handler: (data: T["eventMap"][E], queryClient: QueryClient) => void,
    ): void;

    /** Connection liveness; `recovered === false` after a reconnect is a good
     *  moment to invalidate stale queries. */
    useConnection(): { connected: boolean; recovered: boolean };
}

// --- implementation ----------------------------------------------------------

export function createReactClient<C extends AnyChannel>(
    url: string,
    path: InferClient<C>["address"],
    options?: WebsocketAsyncAPIOptions<
        // biome-ignore lint/suspicious/noExplicitAny: query/headers loosened here
        any,
        // biome-ignore lint/suspicious/noExplicitAny: query/headers loosened here
        any
    >,
): ReactClient<InferClient<C>> {
    const client = createClient<C>(url, path, options);
    const keyPrefix = `wsaa:${path as string}`;
    // biome-ignore lint/suspicious/noExplicitAny: hooks are typed via ReactClient
    const c = client as any;

    function useRequest(command: string, input: unknown, opts?: object) {
        return useQuery({
            queryKey: [keyPrefix, "rpc", command, input],
            queryFn: () => c.request(command, input),
            ...opts,
        });
    }

    function useMutate(command: string, opts?: object) {
        return useMutation({
            mutationFn: (input: unknown) => c.request(command, input),
            ...opts,
        });
    }

    function usePresence() {
        const [members, setMembers] = useState<Map<string, unknown>>(() =>
            c.presence.get(),
        );
        const [self, setSelf] = useState<string | null>(c.presence.self);
        useEffect(() => {
            const unsub = c.presence.subscribe((m: Map<string, unknown>) => {
                setMembers(m);
                setSelf(c.presence.self);
            });
            return unsub;
        }, []);
        return {
            members,
            self,
            set: c.presence.set,
            clear: c.presence.clear,
        };
    }

    function useHistory(
        room: string,
        opts?: { liveEvent?: string; limit?: number },
    ) {
        const qc = useQueryClient();
        const liveEvent = opts?.liveEvent;
        const limit = opts?.limit;
        const queryKey = [keyPrefix, "history", room, limit];
        const query = useQuery({
            queryKey,
            queryFn: () => c.history(room, { limit }),
        });
        useEffect(() => {
            if (!liveEvent) return;
            const unsub = c.onEvent(liveEvent, (data: unknown) => {
                qc.setQueryData(
                    queryKey,
                    (old: Array<{ event: string; data: unknown }> = []) => {
                        const next = [...old, { event: liveEvent, data }];
                        return limit != null ? next.slice(-limit) : next;
                    },
                );
            });
            return unsub;
            // queryKey members are listed individually below
        }, [room, liveEvent, limit, qc]);
        return query;
    }

    function useStream(
        name: string,
        input: unknown,
        opts?: StreamReduce<unknown, unknown>,
    ): StreamResult<unknown> {
        const fold = streamFold(opts);
        const [data, setData] = useState<unknown>(fold.initial);
        const [isDone, setIsDone] = useState(false);
        const [error, setError] = useState<RpcError | null>(null);
        const inputKey = JSON.stringify(input ?? null);
        useEffect(() => {
            let cancelled = false;
            setData(fold.initial);
            setIsDone(false);
            setError(null);
            const iter = c
                .stream(name, input)
                [Symbol.asyncIterator]() as AsyncIterator<unknown>;
            void (async () => {
                try {
                    while (true) {
                        const { value, done } = await iter.next();
                        if (cancelled || done) break;
                        setData((prev: unknown) => fold.step(prev, value));
                    }
                    if (!cancelled) setIsDone(true);
                } catch (e) {
                    if (!cancelled)
                        setError(
                            e instanceof RpcError
                                ? e
                                : new RpcError("INTERNAL", String(e)),
                        );
                }
            })();
            return () => {
                cancelled = true;
                // breaking iteration sends a StreamStop so the server cancels
                void iter.return?.(undefined);
            };
            // eslint-disable-next-line react-hooks/exhaustive-deps
        }, [name, inputKey]);
        return { data, isDone, error };
    }

    function useLastEvent(event: string) {
        const [value, setValue] = useState<unknown>(undefined);
        useEffect(() => c.onEvent(event, setValue), [event]);
        return value;
    }

    function useEvent(
        event: string,
        handler: (data: unknown, qc: QueryClient) => void,
    ) {
        const qc = useQueryClient();
        const ref = useRef(handler);
        ref.current = handler;
        useEffect(
            () => c.onEvent(event, (data: unknown) => ref.current(data, qc)),
            [event, qc],
        );
    }

    function useConnection() {
        const [connected, setConnected] = useState<boolean>(c.connected);
        const [recovered, setRecovered] = useState<boolean>(c.recovered);
        useEffect(() => {
            const offOpen = c.onOpen(() => setConnected(true));
            const offClose = c.onClose(() => setConnected(false));
            const offRecover = c.onRecover((r: boolean) => {
                setRecovered(r);
                setConnected(true);
            });
            return () => {
                offOpen();
                offClose();
                offRecover();
            };
        }, []);
        return { connected, recovered };
    }

    return {
        client,
        useRequest,
        useMutate,
        usePresence,
        useHistory,
        useStream,
        useLastEvent,
        useEvent,
        useConnection,
        // biome-ignore lint/suspicious/noExplicitAny: runtime is typed via ReactClient
    } as any;
}
