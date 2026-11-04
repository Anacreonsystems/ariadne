import type { Meta, StoryObj } from "@storybook/react";
import { buildApiUrl } from "@tests/utils/handlers";
import { delay, HttpResponse, http } from "msw";
import {
	createFakeNativeStreamProjection,
	createFakeReleasedNativeStreamProjection,
} from "@/mocks/create-fake-native-stream-projection";
import { reactQueryDecorator, routerDecorator } from "@/storybook/utils";
import { NativeStreamProgress } from ".";

const endpoint = buildApiUrl("/streams/v1/deployments/:id/projection");
const projection = createFakeNativeStreamProjection();

const meta = {
	title: "Components/FlowRuns/NativeStreamProgress",
	component: NativeStreamProgress,
	decorators: [reactQueryDecorator, routerDecorator],
	args: { flowRunId: projection.flow_run_id },
	parameters: {
		msw: {
			handlers: [http.get(endpoint, () => HttpResponse.json(projection))],
		},
	},
} satisfies Meta<typeof NativeStreamProgress>;
export default meta;
type Story = StoryObj<typeof meta>;

export const Running: Story = {};
export const Draining: Story = {
	parameters: {
		msw: {
			handlers: [
				http.get(endpoint, () =>
					HttpResponse.json(
						createFakeNativeStreamProjection({ source_admission_closed: true }),
					),
				),
			],
		},
	},
};
export const Cancelling: Story = {
	parameters: {
		msw: {
			handlers: [
				http.get(endpoint, () =>
					HttpResponse.json(
						createFakeNativeStreamProjection({
							source_admission_closed: true,
							cancel_requested: true,
						}),
					),
				),
			],
		},
	},
};
export const AwaitingWorker: Story = {
	parameters: {
		msw: {
			handlers: [
				http.get(endpoint, () =>
					HttpResponse.json(
						createFakeNativeStreamProjection({
							generation: null,
							authority_state: null,
							prepared_generation: null,
							manifest_sealed: false,
							launch_state: null,
							worker_id: null,
							items: [],
							finite_items: [],
						}),
					),
				),
			],
		},
	},
};
export const Unavailable: Story = {
	parameters: {
		msw: {
			handlers: [
				http.get(endpoint, () =>
					HttpResponse.json(
						{ detail: "Projection unavailable" },
						{ status: 500 },
					),
				),
			],
		},
	},
};
export const Loading: Story = {
	parameters: {
		msw: {
			handlers: [
				http.get(endpoint, async () => {
					await delay("infinite");
					return HttpResponse.json(projection);
				}),
			],
		},
	},
};

export const Released: Story = {
	parameters: {
		msw: {
			handlers: [
				http.get(endpoint, () =>
					HttpResponse.json(createFakeReleasedNativeStreamProjection()),
				),
			],
		},
	},
};
