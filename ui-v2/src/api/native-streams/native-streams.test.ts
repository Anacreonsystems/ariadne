import { useQuery } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { buildApiUrl, createWrapper, server } from "@tests/utils";
import { HttpResponse, http } from "msw";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createFakeNativeStreamProjection } from "@/mocks/create-fake-native-stream-projection";
import {
	buildNativeStreamProjectionQuery,
	queryKeyFactory,
	useNativeStreamControl,
} from ".";

const endpoint = buildApiUrl("/streams/v1/deployments/:id/projection");

afterEach(() => vi.restoreAllMocks());

describe("native stream projection", () => {
	it("uses the shared authenticated transport and returns the server observation", async () => {
		const observation = createFakeNativeStreamProjection();
		const request = vi.fn();
		vi.spyOn(localStorage, "getItem").mockImplementation((key) =>
			key === "prefect-password" ? "native-reader" : null,
		);
		server.use(
			http.get(endpoint, ({ request: req, params }) => {
				request(req.headers.get("Authorization"), params.id);
				return HttpResponse.json(observation);
			}),
		);
		const { result } = renderHook(
			() => useQuery(buildNativeStreamProjectionQuery(observation.flow_run_id)),
			{
				wrapper: createWrapper(),
			},
		);
		await waitFor(() => expect(result.current.isSuccess).toBe(true));
		expect(result.current.data).toEqual(observation);
		expect(request).toHaveBeenCalledWith(
			"Basic native-reader",
			observation.flow_run_id,
		);
	});

	it.each([404, 409, 500])(
		"does not turn HTTP %s into empty successful progress",
		async (status) => {
			server.use(
				http.get(endpoint, () =>
					HttpResponse.json({ detail: "Projection unavailable" }, { status }),
				),
			);
			const { result } = renderHook(
				() => useQuery(buildNativeStreamProjectionQuery("native-run")),
				{
					wrapper: createWrapper(),
				},
			);
			await waitFor(() => expect(result.current.isError).toBe(true));
			expect(result.current.data).toBeUndefined();
		},
	);

	it("names observations by FlowRun and refreshes live data at a bounded interval", () => {
		const options = buildNativeStreamProjectionQuery("run-1");
		expect(options.queryKey).toEqual(queryKeyFactory.detail("run-1"));
		expect(options.refetchInterval).toBe(30_000);
		expect(
			buildNativeStreamProjectionQuery("run-1", 5000).refetchInterval,
		).toBe(5000);
	});
});

describe("native lifecycle requests", () => {
	it("reuses one operation after an unconfirmed response and never sends holder credentials", async () => {
		const bodies: unknown[] = [];
		const privateHeader = vi.fn();
		let attempts = 0;
		server.use(
			http.post(
				buildApiUrl("/streams/v1/deployments/control/drain"),
				async ({ request }) => {
					const body: unknown = await request.json();
					bodies.push(body);
					privateHeader(
						request.headers.get("X-Prefect-Stream-Execution-Capability"),
					);
					attempts += 1;
					return attempts === 1
						? HttpResponse.json(
								{ detail: "Response unavailable" },
								{ status: 500 },
							)
						: HttpResponse.json({
								flow_run_id: "native-run",
								operation_id: "operation",
								generation: 2,
								kind: "DRAIN",
								source_admission_closed: true,
								cancel_requested: false,
								created_at: "2026-11-04T12:00:00Z",
							});
				},
			),
		);
		const { result } = renderHook(() => useNativeStreamControl(), {
			wrapper: createWrapper(),
		});
		act(() =>
			result.current.requestControl({
				flowRunId: "native-run",
				expectedGeneration: 2,
				kind: "drain",
			}),
		);
		await waitFor(() => expect(result.current.isError).toBe(true));
		act(() =>
			result.current.requestControl({
				flowRunId: "native-run",
				expectedGeneration: 2,
				kind: "drain",
			}),
		);
		await waitFor(() => expect(result.current.isSuccess).toBe(true));
		expect(bodies).toHaveLength(2);
		expect(bodies[0]).toEqual(bodies[1]);
		expect(bodies[0]).toMatchObject({
			flow_run_id: "native-run",
			expected_generation: 2,
			reason: "Requested from the FlowRun page",
		});
		expect(privateHeader).toHaveBeenNthCalledWith(1, null);
	});
});
