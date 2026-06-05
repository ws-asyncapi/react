/**
 * Typed React hooks for ws-asyncapi — a thin binding over the framework-agnostic
 * `@ws-asyncapi/query-core` (which holds the query/mutation options, the
 * live-cache glue, the stream fold, and the subscribable presence/stream/event
 * stores). The same core powers future `@ws-asyncapi/solid` / `vue` / `svelte`
 * bindings; only this hook layer is React-specific.
 *
 * ```tsx
 * import { createReactClient } from "@ws-asyncapi/react";
 * import type { chat } from "./server";
 *
 * export const ws = createReactClient<typeof chat>("ws://localhost:3000", "/chat/1");
 * const { data } = ws.useHistory("room:42", { liveEvent: "message", limit: 50 });
 * const { members, set } = ws.usePresence();
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
import {
    createClient,
    type HistoryEntry,
    type RpcError,
    type TypedRpcError,
    type WebsocketAsyncAPIOptions,
} from "@ws-asyncapi/client";
import {
    connectionStore,
    historyQueryOptions,
    lastEventStore,
    mutationOptions,
    presenceStore,
    type QueryCoreClient,
    requestQueryOptions,
    type StreamReduce,
    streamStore,
    subscribeHistoryLive,
} from "@ws-asyncapi/query-core";
import { useEffect, useMemo, useRef, useSyncExternalStore } from "react";
import type { AnyChannel, InferClient } from "ws-asyncapi";

// re-export the pure fold + its type for convenience / testing
export { streamFold, type StreamReduce } from "@ws-asyncapi/query-core";

/** The shape inferred from a channel — what the hooks are typed against. */
type Shape = InferClient<AnyChannel>;

export interface StreamResult<Data> {
    data: Data;
    isDone: boolean;
    error: RpcError | null;
}

export interface PresenceResult<State> {
    members: Map<string, State>;
    self: string | null;
    set: (state: State) => Promise<void>;
    clear: () => Promise<void>;
}

export interface ReactClient<T extends Shape> {
    /** the underlying client (escape hatch: `opened`, raw `request`, …) */
    client: ReturnType<typeof createClient<AnyChannel>>;

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

    usePresence(): PresenceResult<T["presenceState"]>;

    useHistory(
        room: string,
        options?: { liveEvent?: keyof T["eventMap"] & string; limit?: number },
    ): UseQueryResult<HistoryEntry<T["eventMap"]>[], RpcError>;

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

    useLastEvent<E extends keyof T["eventMap"]>(
        event: E,
    ): T["eventMap"][E] | undefined;

    useEvent<E extends keyof T["eventMap"]>(
        event: E,
        handler: (data: T["eventMap"][E], queryClient: QueryClient) => void,
    ): void;

    useConnection(): { connected: boolean; recovered: boolean };
}

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
    const core = client as unknown as QueryCoreClient;

    // per-client singletons (stable stores for useSyncExternalStore)
    const presence = presenceStore(core);
    const connection = connectionStore(core);

    function useRequest(command: string, input: unknown, opts?: object) {
        return useQuery({
            ...requestQueryOptions(core, keyPrefix, command, input),
            ...opts,
        });
    }

    function useMutate(command: string, opts?: object) {
        return useMutation({ ...mutationOptions(core, command), ...opts });
    }

    function usePresence() {
        const snap = useSyncExternalStore(
            presence.subscribe,
            presence.getSnapshot,
            presence.getSnapshot,
        );
        return {
            members: snap.members,
            self: snap.self,
            set: presence.set,
            clear: presence.clear,
        };
    }

    function useHistory(
        room: string,
        opts?: { liveEvent?: string; limit?: number },
    ) {
        const qc = useQueryClient();
        const liveEvent = opts?.liveEvent;
        const limit = opts?.limit;
        const query = useQuery(historyQueryOptions(core, keyPrefix, room, limit));
        useEffect(() => {
            if (!liveEvent) return;
            return subscribeHistoryLive(core, qc, keyPrefix, room, {
                liveEvent,
                limit,
            });
        }, [room, liveEvent, limit, qc]);
        return query;
    }

    function useStream(
        name: string,
        input: unknown,
        opts?: StreamReduce<unknown, unknown>,
    ): StreamResult<unknown> {
        const inputKey = JSON.stringify(input ?? null);
        // a stable mode key so re-renders don't recreate the store (which would
        // restart the stream); fold options are read once when the store is made
        const mode = !opts
            ? "latest"
            : opts.reduce === "append"
              ? `append:${opts.max ?? ""}`
              : "custom";
        const store = useMemo(
            () => streamStore(core, name, input, opts),
            // eslint-disable-next-line react-hooks/exhaustive-deps
            [name, inputKey, mode],
        );
        return useSyncExternalStore(
            store.subscribe,
            store.getSnapshot,
            store.getSnapshot,
        );
    }

    function useLastEvent(event: string) {
        const store = useMemo(() => lastEventStore(core, event), [event]);
        return useSyncExternalStore(
            store.subscribe,
            store.getSnapshot,
            store.getSnapshot,
        );
    }

    function useEvent(
        event: string,
        handler: (data: unknown, qc: QueryClient) => void,
    ) {
        const qc = useQueryClient();
        const ref = useRef(handler);
        ref.current = handler;
        useEffect(
            () => core.onEvent(event, (data: unknown) => ref.current(data, qc)),
            [event, qc],
        );
    }

    function useConnection() {
        return useSyncExternalStore(
            connection.subscribe,
            connection.getSnapshot,
            connection.getSnapshot,
        );
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
        // biome-ignore lint/suspicious/noExplicitAny: runtime typed via ReactClient
    } as any;
}
