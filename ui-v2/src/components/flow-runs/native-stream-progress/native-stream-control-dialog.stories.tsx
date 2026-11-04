import type { Meta, StoryObj } from "@storybook/react";
import { buildApiUrl } from "@tests/utils/handlers";
import { HttpResponse, http } from "msw";
import { fn } from "storybook/test";
import { createFakeNativeStreamProjection } from "@/mocks/create-fake-native-stream-projection";
import {
	reactQueryDecorator,
	routerDecorator,
	toastDecorator,
} from "@/storybook/utils";
import { NativeStreamControlDialog } from "./native-stream-control-dialog";

const projection = createFakeNativeStreamProjection();
const endpoint = buildApiUrl("/streams/v1/deployments/:id/projection");
const meta = {
	title: "Components/FlowRuns/NativeStreamControlDialog",
	component: NativeStreamControlDialog,
	decorators: [reactQueryDecorator, routerDecorator, toastDecorator],
	args: {
		flowRunId: projection.flow_run_id,
		kind: "drain",
		open: true,
		onOpenChange: fn(),
	},
	parameters: {
		msw: {
			handlers: [http.get(endpoint, () => HttpResponse.json(projection))],
		},
	},
} satisfies Meta<typeof NativeStreamControlDialog>;
export default meta;
type Story = StoryObj<typeof meta>;

export const Drain: Story = {};
export const Cancel: Story = { args: { kind: "cancel" } };
export const AwaitingEnrollment: Story = {
	parameters: {
		msw: {
			handlers: [
				http.get(endpoint, () =>
					HttpResponse.json(
						createFakeNativeStreamProjection({
							generation: null,
							authority_state: null,
							worker_id: null,
							launch_state: null,
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
