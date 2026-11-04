import { DEFAULT_ROOT_ARTIFACT_Z_INDEX } from "@/graphs/consts";
import { runArtifactsFactory } from "@/graphs/factories/runArtifacts";
import type { RunGraphData } from "@/graphs/models/RunGraph";
import { waitForApplication } from "@/graphs/objects/application";
import { emitter } from "@/graphs/objects/events";
import { waitForRunData } from "@/graphs/objects/nodes";
import { layout, waitForSettings } from "@/graphs/objects/settings";

let scope = 0;
let stopOwner: (() => void) | null = null;

export async function startFlowRunArtifacts(): Promise<void> {
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
	const data = await waitForRunData();
	if (!isActive()) {
		return;
	}
	const settings = await waitForSettings();
	if (!isActive()) {
		return;
	}
	const {
		element,
		render: renderArtifacts,
		update,
	} = await runArtifactsFactory({ isRoot: true });
	if (!isActive()) {
		element.destroy({ children: true });
		return;
	}

	element.zIndex = DEFAULT_ROOT_ARTIFACT_Z_INDEX;
	stopOwner = () => element.destroy({ children: true });

	function render(newData?: RunGraphData): void {
		if (!isActive()) {
			return;
		}
		if (layout.isTemporal() && !settings.disableArtifacts) {
			stage.addChild(element);
			renderArtifacts(newData?.artifacts);
			return;
		}

		stage.removeChild(element);
	}

	if (data.artifacts) {
		render(data);
	}

	const offViewportMoved = emitter.on("viewportMoved", () => update());

	const offDataCreated = emitter.on("runDataCreated", (data) => render(data));
	const offDataUpdated = emitter.on("runDataUpdated", (data) => render(data));
	const offConfigUpdated = emitter.on("configUpdated", () => render());
	const offLayoutUpdated = emitter.on("layoutSettingsUpdated", () => render());
	element.once("destroyed", () => {
		offViewportMoved();
		offDataCreated();
		offDataUpdated();
		offConfigUpdated();
		offLayoutUpdated();
	});
}

export function stopFlowRunArtifacts(): void {
	scope++;
	stopOwner?.();
	stopOwner = null;
}
