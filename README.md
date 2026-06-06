# @ws-asyncapi/react

Typed **React hooks** for [ws-asyncapi](https://github.com/ws-asyncapi), inferred
straight from your server `Channel` — no codegen. The design **bridges to
[TanStack Query](https://tanstack.com/query)** rather than building a second
cache: RPCs become queries/mutations, and server events/streams patch the cache
so data stays live. Presence and streams use plain React state.

## Installation

```bash
npm install @ws-asyncapi/react
# peers: react >=18, @tanstack/react-query ^5, @ws-asyncapi/client, ws-asyncapi
```

## Setup

```tsx
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createReactClient } from "@ws-asyncapi/react";
import type { chat } from "./server"; // the Channel value's type

export const ws = createReactClient<typeof chat>("ws://localhost:3000", "/chat/1");
const qc = new QueryClient();

export function App() {
  return (
    <QueryClientProvider client={qc}>
      <Room />
    </QueryClientProvider>
  );
}
```

## Hooks

```tsx
// RPC as a query (read) — keyed by [path, "rpc", name, input]
const { data, isLoading } = ws.useRequest("getRoom", { id: "42" });

// RPC as a mutation (write) — error is the typed RpcError union
const send = ws.useMutate("sendMessage");
send.mutate({ text: "hi" });

// room history + live: fetch the backlog AND append incoming events, one cache
const { data: messages } = ws.useHistory("room:42", {
  liveEvent: "message",
  limit: 50,
});

// presence: live roster + your own state
const { members, self, set } = ws.usePresence();

// stream — LATEST value by default (O(1), nothing accumulated)
const { data: price } = ws.useStream("prices", { symbol: "ACME" });

// stream — opt into a bounded list…
const { data: ticks } = ws.useStream("prices", { symbol: "ACME" }, {
  reduce: "append",
  max: 100,
});
// …or a custom fold (no item retention)
const { data: total } = ws.useStream("prices", { symbol: "ACME" }, {
  reduce: (acc, t) => acc + t.price,
  initial: 0,
});

// the most recent value of an event (stores one value)
const status = ws.useLastEvent("status");

// run a side effect / patch the cache on each event (stores nothing)
ws.useEvent("message", (msg, queryClient) => { /* ... */ });

// connection liveness — recovered === false is a good time to invalidate
const { connected, recovered } = ws.useConnection();
```

Everything is typed from the channel: hook names, payloads, results, presence
state, and stream outputs are all inferred, and wrong names/payloads are compile
errors.

### A chat component, end to end

```tsx
function Room() {
  const { data: messages } = ws.useHistory("room:42", { liveEvent: "message", limit: 50 });
  const { members } = ws.usePresence();
  const send = ws.useMutate("sendMessage");

  return (
    <>
      <Presence members={members} />
      <List items={messages ?? []} />
      <Composer onSend={(text) => send.mutate({ text })} disabled={send.isPending} />
    </>
  );
}
```

## Design notes

- **No second cache.** `useRequest`/`useHistory` are thin wrappers over
  `useQuery`; events/streams call `setQueryData`. Devtools, retries, suspense,
  and SSR hydration keep working because it *is* TanStack Query.
- **Latest-by-default streams.** Accumulating every yielded value is opt-in
  (`reduce: "append"` or a custom fold), so the common "I just need the current
  value" case never grows an array.
- **`recovered` drives correctness.** After a reconnect that didn't recover, use
  `useConnection().recovered === false` to `invalidateQueries`.

The underlying client is exposed as `ws.client` for escape hatches (`opened`,
raw `request`, `onRequest`, …).

## License

MIT
