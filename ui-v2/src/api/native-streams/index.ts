import {
	queryOptions,
	useMutation,
	useQueryClient,
} from "@tanstack/react-query";
import { useRef } from "react";
import { queryKeyFactory as flowRunsQueryKeyFactory } from "@/api/flow-runs";
import type { components } from "@/api/prefect";
import { getQueryService } from "@/api/service";

export type NativeStreamFlowProjection =
	components["schemas"]["NativeStreamFlowProjection"];
export type NativeStreamTaskProjection =
	components["schemas"]["NativeStreamTaskProjection"];
export type NativeStreamFiniteProjection =
	components["schemas"]["NativeStreamFiniteProjection"];

export const queryKeyFactory = {
	all: () => ["nativeStreams"] as const,
	detail: (flowRunId: string) =>
		[...queryKeyFactory.all(), "projection", flowRunId] as const,
};

export const buildNativeStreamProjectionQuery = (
	flowRunId: string,
	refetchInterval = 30_000,
) =>
	queryOptions({
		queryKey: queryKeyFactory.detail(flowRunId),
		queryFn: async () => {
			const response = await (await getQueryService()).GET(
				"/streams/v1/deployments/{flow_run_id}/projection",
				{
					params: { path: { flow_run_id: flowRunId } },
				},
			);
			if (!response.data)
				throw new Error("Streaming progress response was empty");
			return response.data;
		},
		refetchInterval,
		staleTime: 1000,
		retry: false,
	});

export type NativeStreamControlResult =
	components["schemas"]["NativeStreamControlResult"];
export type NativeStreamControlKind = "drain" | "cancel";
type NativeControlIntent = {
	flowRunId: string;
	expectedGeneration: number;
	kind: NativeStreamControlKind;
};

export function useNativeStreamControl() {
	const queryClient = useQueryClient();
	const operation = useRef<{ key: string; id: string } | null>(null);
	const mutation = useMutation({
		mutationFn: async ({
			flowRunId,
			expectedGeneration,
			kind,
		}: NativeControlIntent) => {
			const key = `${flowRunId}/${expectedGeneration}/${kind}`;
			let operationId =
				operation.current?.key === key ? operation.current.id : undefined;
			if (!operationId) {
				operationId = crypto.randomUUID();
				operation.current = { key, id: operationId };
			}
			const response = await (await getQueryService()).POST(
				kind === "cancel"
					? "/streams/v1/deployments/control/cancel"
					: "/streams/v1/deployments/control/drain",
				{
					body: {
						flow_run_id: flowRunId,
						operation_id: operationId,
						expected_generation: expectedGeneration,
						reason: "Requested from the FlowRun page",
					},
				},
			);
			if (!response.data)
				throw new Error("The server has not confirmed the streaming request");
			return response.data;
		},
		onSettled: async (_data, _error, { flowRunId }) => {
			await Promise.all([
				queryClient.invalidateQueries({
					queryKey: queryKeyFactory.detail(flowRunId),
				}),
				queryClient.invalidateQueries({
					queryKey: flowRunsQueryKeyFactory.detail(flowRunId),
				}),
				queryClient.invalidateQueries({
					queryKey: flowRunsQueryKeyFactory.lists(),
				}),
			]);
		},
		retry: false,
	});
	return { ...mutation, requestControl: mutation.mutate };
}
