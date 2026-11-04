import { DEFAULT_ROOT_EVENT_Z_INDEX } from "@/graphs/consts";
import { eventDataFactory } from "@/graphs/factories/eventData";
import { runEventsFactory } from "@/graphs/factories/runEvents";
import type { RunGraphEvent } from "@/graphs/models";
import { waitForApplication } from "@/graphs/objects/application";
import { waitForConfig } from "@/graphs/objects/config";
import { type EventKey, emitter, waitForEvent } from "@/graphs/objects/events";
import { waitForRunData } from "@/graphs/objects/nodes";
import { layout, waitForSettings } from "@/graphs/objects/settings";

let stopEventData: (() => void) | null = null;
let rootGraphEvents: RunGraphEvent[] | null = null;
let scope = 0;

export async function startFlowRunEvents(): Promise<void> {
	const startedScope = ++scope;
	const application = await waitForApplication();
	if (startedScope !== scope) {
		return;
	}
	const stage = application.stage;
	const isActive = (): boolean => startedScope === scope && !!stage && !stage.destroyed;
	if (!isActive()) {
		return;
	}
	const config = await waitForConfig();
	if (!isActive()) {
		return;
	}
	const settings = await waitForSettings();
	if (!isActive()) {
		return;
	}
	const data = await waitForRunData();
	if (!isActive()) {
		return;
	}

	const {
		element,
		render: renderEvents,
		update,
	} = await runEventsFactory({ isRoot: true });
	if (!isActive()) {
		element.destroy({ children: true });
		return;
	}

	element.zIndex = DEFAULT_ROOT_EVENT_Z_INDEX;
	stopEventData = () => element.destroy({ children: true });

	async function render(data?: RunGraphEvent[]): Promise<void> {
		if (!isActive()) {
			return;
		}
		if (!layout.isTemporal() || settings.disableEvents) {
			stage.removeChild(element);
			return;
		}

		stage.addChild(element);

		await renderEvents(data);
	}

	const response = await eventDataFactory(
		() => ({
			nodeId: config.runId,
			since: data.start_time,
			until: data.end_time ?? new Date(),
		}),
		(data) => {
			if (!isActive()) {
				return;
			}
			const event: EventKey = rootGraphEvents
				? "eventDataUpdated"
				: "eventDataCreated";

			rootGraphEvents = data;

			emitter.emit(event, rootGraphEvents);

			render(data);
		},
	);
	if (!isActive()) {
		response.stop();
		element.destroy({ children: true });
		return;
	}

	const offConfigUpdated = emitter.on("configUpdated", () => render());
	const offViewportMoved = emitter.on("viewportMoved", () => update());
	const offLayoutUpdated = emitter.on("layoutSettingsUpdated", () => render());
	element.once("destroyed", () => {
		response.stop();
		offConfigUpdated();
		offViewportMoved();
		offLayoutUpdated();
	});

	stopEventData = () => {
		response.stop();
		element.destroy({ children: true });
	};

	response.start();
}

export function stopFlowRunEvents(): void {
	scope++;
	stopEventData?.();
	stopEventData = null;
	rootGraphEvents = null;
}

export async function waitForRunEvents(): Promise<RunGraphEvent[] | null> {
	if (rootGraphEvents) {
		return rootGraphEvents;
	}

	return await waitForEvent("eventDataCreated");
}
