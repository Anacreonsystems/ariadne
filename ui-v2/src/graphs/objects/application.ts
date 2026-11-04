import { Application } from "pixi.js";
import { emitter, waitForEvent } from "@/graphs/objects/events";
import { waitForStage } from "@/graphs/objects/stage";

export let application: Application | null = null;
let generation = 0;
let initialization: { generation: number; promise: Promise<void> } | null = null;

export async function startApplication(): Promise<void> {
	const startedGeneration = generation;
	const stage = await waitForStage();
	if (startedGeneration !== generation) {
		return;
	}

	await createApplication(stage, startedGeneration);
	if (startedGeneration !== generation) {
		return;
	}

	emitter.on("stageUpdated", resizeApplication);
}

export function stopApplication(): void {
	generation++;
	const ownedApplication = application;
	application = null;
	ownedApplication?.destroy(true, { children: true });
}

async function createApplication(stage: HTMLDivElement, startedGeneration: number): Promise<void> {
	if (application) {
		return;
	}

	if (initialization?.generation === startedGeneration) {
		return await initialization.promise;
	}
	const ownedInitialization = {
		generation: startedGeneration,
		promise: initializeApplication(stage, startedGeneration),
	};
	initialization = ownedInitialization;
	try {
		await ownedInitialization.promise;
	} finally {
		if (initialization === ownedInitialization) {
			initialization = null;
		}
	}
}

async function initializeApplication(stage: HTMLDivElement, startedGeneration: number): Promise<void> {
	const ownedApplication = new Application();

	// PixiJS v8 requires async initialization before accessing properties
	await ownedApplication.init({
		backgroundAlpha: 0,
		resizeTo: stage,
		antialias: true,
		resolution: Math.ceil(window.devicePixelRatio),
	});
	if (startedGeneration !== generation) {
		ownedApplication.destroy(true, { children: true });
		return;
	}

	// for setting the viewport above the guides
	ownedApplication.stage.sortableChildren = true;

	stage.appendChild(ownedApplication.canvas);

	application = ownedApplication;
	emitter.emit("applicationCreated", ownedApplication);

	if (process.env.NODE_ENV === "development") {
		// For whatever reason typing globalThis is not quite working and not worth the time to fix for devtools
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		(globalThis as any).__PIXI_APP__ = application;
	}
}

export async function waitForApplication(): Promise<Application> {
	if (application) {
		return application;
	}

	return await waitForEvent("applicationCreated");
}

function resizeApplication(stage: HTMLDivElement): void {
	if (!application) {
		return;
	}

	application.resizeTo = stage;
	emitter.emit("applicationResized", application);
}
