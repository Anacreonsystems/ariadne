import type {
	RunGraphEvent,
	RunGraphFetchEventsContext,
} from "@/graphs/models";
import type { MaybeGetter } from "@/graphs/models/utilities";
import { waitForConfig } from "@/graphs/objects/config";
import { waitForRunData } from "@/graphs/objects/nodes";
import { toValue } from "@/graphs/utilities";

type EventDataCallback = (data: RunGraphEvent[]) => void;

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export async function eventDataFactory(
	context: MaybeGetter<RunGraphFetchEventsContext>,
	callback: EventDataCallback,
) {
	const runGraphData = await waitForRunData();
	const config = await waitForConfig();

	let interval: ReturnType<typeof setInterval> | undefined;
	let data: RunGraphEvent[] | null = null;
	let generation = 0;

	async function start(): Promise<void> {
		const startedGeneration = ++generation;
		clearTimeout(interval);
		try {
			const response = await config.fetchEvents(toValue(context));
			if (startedGeneration !== generation) {
				return;
			}
			data = response;

			callback(data);
		} catch (error) {
			if (startedGeneration === generation) {
				console.error(error);
			}
		}

		if (startedGeneration === generation && !runGraphData.end_time) {
			interval = setTimeout(() => start(), config.fetchEventsInterval);
		}
	}

	function stop(): void {
		generation++;
		clearTimeout(interval);
	}

	return {
		start,
		stop,
	};
}
